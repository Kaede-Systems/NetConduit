/// Node identity and message signing using Ed25519.
///
/// Ed25519 provides 128-bit security (equivalent to RSA-3072+) with:
///   - 32-byte public keys
///   - 64-byte signatures
///   - ~50 µs signing and ~100 µs verification on modern CPUs
///
/// # Usage
///
/// ```no_run
/// use netconduit::security::NodeIdentity;
/// let id = NodeIdentity::generate().unwrap();
/// let sig = id.sign(b"hello");
/// assert!(NodeIdentity::verify(&id.public_key, b"hello", &sig));
/// ```
// aws-lc-rs mirrors the ring API but uses hardware-accelerated primitives:
// VAES (vectorized AES) for key derivation, SHA-NI for hashing during signing,
// and constant-time Ed25519 assembly validated against WycheProof test vectors.
use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::signature::{Ed25519KeyPair, KeyPair, UnparsedPublicKey, ED25519};

// ─── NodeIdentity ─────────────────────────────────────────────────────────────

/// An Ed25519 keypair representing a node's identity.
///
/// The `peer_id` is a stable 32-hex-char identifier derived from the first
/// 16 bytes of the public key. Use it as the `Packet.src_id` when signing.
pub struct NodeIdentity {
    /// Stable 32-char hex identifier (first 16 bytes of public key).
    pub peer_id:    String,
    keypair:        Ed25519KeyPair,
    /// PKCS#8 v2 DER blob — persist this to restore the identity across restarts.
    pkcs8:          Vec<u8>,
    /// Raw 32-byte Ed25519 public key. Distribute to peers for signature verification.
    pub public_key: Vec<u8>,
}

impl NodeIdentity {
    /// Generate a fresh Ed25519 keypair.
    pub fn generate() -> anyhow::Result<Self> {
        let rng = SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng)
            .map_err(|e| anyhow::anyhow!("key generation failed: {e:?}"))?
            .as_ref()
            .to_vec();
        Self::from_pkcs8(pkcs8)
    }

    /// Restore from a previously serialized PKCS#8 DER blob.
    pub fn from_pkcs8(pkcs8: Vec<u8>) -> anyhow::Result<Self> {
        let keypair = Ed25519KeyPair::from_pkcs8(&pkcs8)
            .map_err(|e| anyhow::anyhow!("invalid PKCS#8: {e:?}"))?;
        let pub_key = keypair.public_key().as_ref().to_vec();
        let peer_id = hex::encode(&pub_key[..16]);
        Ok(Self { peer_id, keypair, pkcs8, public_key: pub_key })
    }

    /// Sign `data`. Returns a 64-byte Ed25519 signature.
    pub fn sign(&self, data: &[u8]) -> Vec<u8> {
        self.keypair.sign(data).as_ref().to_vec()
    }

    /// Verify that `signature` over `data` was produced by the holder of `public_key`.
    pub fn verify(public_key: &[u8], data: &[u8], signature: &[u8]) -> bool {
        let pk = UnparsedPublicKey::new(&ED25519, public_key);
        pk.verify(data, signature).is_ok()
    }

    /// PKCS#8 DER bytes — persist these to restore the identity later.
    pub fn pkcs8_bytes(&self) -> &[u8] { &self.pkcs8 }
}

impl std::fmt::Debug for NodeIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeIdentity")
            .field("peer_id", &self.peer_id)
            .field("public_key_hex", &hex::encode(&self.public_key))
            .finish()
    }
}

// ─── Packet signing helpers ───────────────────────────────────────────────────

use crate::core::FLAG_SIGNED;
use crate::protocol::Packet;

/// Attach an Ed25519 signature to `pkt.signature` and set `FLAG_SIGNED`.
/// The signature covers `pkt.payload` only — change payload, change signature.
pub fn sign_packet(pkt: &mut Packet, identity: &NodeIdentity) {
    pkt.src_id    = identity.peer_id.clone();
    pkt.signature = identity.sign(&pkt.payload);
    pkt.flags    |= FLAG_SIGNED;
}

/// Verify the Ed25519 signature on `pkt` against `public_key`.
/// Returns `true` if the signature is valid OR if the packet is unsigned.
/// Returns `false` if `FLAG_SIGNED` is set but the signature is invalid.
pub fn verify_packet(pkt: &Packet, public_key: &[u8]) -> bool {
    if pkt.flags & FLAG_SIGNED == 0 {
        return true; // unsigned packet — accepted
    }
    NodeIdentity::verify(public_key, &pkt.payload, &pkt.signature)
}

// ─── Key store ────────────────────────────────────────────────────────────────

/// In-memory store of trusted peer public keys.
/// Map `peer_id → public_key_bytes`.
pub struct KeyStore {
    keys: std::sync::RwLock<std::collections::HashMap<String, Vec<u8>>>,
}

impl KeyStore {
    pub fn new() -> Self {
        Self { keys: std::sync::RwLock::new(std::collections::HashMap::new()) }
    }

    /// Register a trusted peer. `peer_id` must match `NodeIdentity::peer_id`.
    pub fn add(&self, peer_id: String, public_key: Vec<u8>) {
        self.keys.write().unwrap().insert(peer_id, public_key);
    }

    pub fn remove(&self, peer_id: &str) {
        self.keys.write().unwrap().remove(peer_id);
    }

    /// Verify `pkt`'s signature if it's signed. Returns `false` if signed but unknown.
    pub fn verify(&self, pkt: &Packet) -> bool {
        if pkt.flags & FLAG_SIGNED == 0 { return true; }
        let keys = self.keys.read().unwrap();
        match keys.get(&pkt.src_id) {
            Some(pk) => NodeIdentity::verify(pk, &pkt.payload, &pkt.signature),
            None     => false, // signed but peer unknown
        }
    }

    /// Returns `true` if `peer_id` is in the store.
    pub fn is_known(&self, peer_id: &str) -> bool {
        self.keys.read().unwrap().contains_key(peer_id)
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_and_sign_verify() {
        let id = NodeIdentity::generate().unwrap();
        let data = b"hello netconduit";
        let sig = id.sign(data);
        assert_eq!(sig.len(), 64, "Ed25519 signature must be 64 bytes");
        assert!(NodeIdentity::verify(&id.public_key, data, &sig));
    }

    #[test]
    fn wrong_data_fails_verify() {
        let id = NodeIdentity::generate().unwrap();
        let sig = id.sign(b"original");
        assert!(!NodeIdentity::verify(&id.public_key, b"tampered", &sig));
    }

    #[test]
    fn wrong_key_fails_verify() {
        let id_a = NodeIdentity::generate().unwrap();
        let id_b = NodeIdentity::generate().unwrap();
        let sig = id_a.sign(b"data");
        assert!(!NodeIdentity::verify(&id_b.public_key, b"data", &sig));
    }

    #[test]
    fn pkcs8_roundtrip() {
        let id = NodeIdentity::generate().unwrap();
        let pkcs8 = id.pkcs8_bytes().to_vec();
        let restored = NodeIdentity::from_pkcs8(pkcs8).unwrap();
        assert_eq!(id.peer_id, restored.peer_id);
        assert_eq!(id.public_key, restored.public_key);
        // Signing with both should produce verifiable signatures.
        let sig = restored.sign(b"test");
        assert!(NodeIdentity::verify(&id.public_key, b"test", &sig));
    }

    #[test]
    fn peer_id_is_32_hex_chars() {
        let id = NodeIdentity::generate().unwrap();
        assert_eq!(id.peer_id.len(), 32);
        assert!(id.peer_id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn packet_sign_and_verify() {
        let id = NodeIdentity::generate().unwrap();
        let mut pkt = Packet {
            version: crate::core::PROTO_VERSION,
            r#type: crate::protocol::PacketType::Message as i32,
            payload: b"important data".to_vec(),
            ..Default::default()
        };
        sign_packet(&mut pkt, &id);
        assert_eq!(pkt.src_id, id.peer_id);
        assert_eq!(pkt.flags & FLAG_SIGNED, FLAG_SIGNED);
        assert!(verify_packet(&pkt, &id.public_key));
    }

    #[test]
    fn packet_tampered_payload_fails_verify() {
        let id = NodeIdentity::generate().unwrap();
        let mut pkt = Packet {
            version: crate::core::PROTO_VERSION,
            payload: b"data".to_vec(),
            ..Default::default()
        };
        sign_packet(&mut pkt, &id);
        pkt.payload = b"tampered".to_vec();
        assert!(!verify_packet(&pkt, &id.public_key));
    }

    #[test]
    fn key_store_verify_known_peer() {
        let id = NodeIdentity::generate().unwrap();
        let store = KeyStore::new();
        store.add(id.peer_id.clone(), id.public_key.clone());

        let mut pkt = Packet {
            version: crate::core::PROTO_VERSION,
            payload: b"secure".to_vec(),
            ..Default::default()
        };
        sign_packet(&mut pkt, &id);
        assert!(store.verify(&pkt));
    }

    #[test]
    fn key_store_rejects_unknown_peer() {
        let id = NodeIdentity::generate().unwrap();
        let store = KeyStore::new(); // empty store

        let mut pkt = Packet {
            version: crate::core::PROTO_VERSION,
            payload: b"secure".to_vec(),
            ..Default::default()
        };
        sign_packet(&mut pkt, &id);
        assert!(!store.verify(&pkt)); // unknown peer → rejected
    }

    #[test]
    fn unsigned_packet_always_accepted() {
        let store = KeyStore::new();
        let pkt = Packet {
            version: crate::core::PROTO_VERSION,
            payload: b"public".to_vec(),
            ..Default::default()
        };
        assert!(store.verify(&pkt));
    }
}
