/// mDNS peer discovery for NetConduit.
///
/// Servers advertise themselves as `_netconduit._udp.local.` services.
/// Clients browse for that service type to find peers on the LAN without
/// any pre-configured addresses.
///
/// Wire format:
///   Service type : `_netconduit._udp.local.`
///   TXT "port"   : decimal port number (redundant with SRV, kept for convenience)
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const SERVICE_TYPE: &str = "_netconduit._udp.local.";

// ─── DiscoveredPeer ───────────────────────────────────────────────────────────

/// A NetConduit peer discovered via mDNS.
#[derive(Debug, Clone)]
pub struct DiscoveredPeer {
    /// mDNS instance name (e.g. "my-server._netconduit._udp.local.").
    pub fullname:  String,
    /// Resolved IP address.
    pub addr:      IpAddr,
    /// QUIC port.
    pub port:      u16,
    /// Friendly hostname from SRV record.
    pub hostname:  String,
    /// Optional TXT properties attached at advertisement time.
    pub properties: HashMap<String, String>,
}

// ─── MdnsAdvertiser ──────────────────────────────────────────────────────────

/// Advertises this NetConduit server on the LAN via mDNS-SD.
///
/// Call `MdnsAdvertiser::new("my-server", port)` right after starting the QUIC
/// server. Drop or call `.stop()` to withdraw the advertisement.
pub struct MdnsAdvertiser {
    daemon:   ServiceDaemon,
    fullname: String,
}

impl MdnsAdvertiser {
    /// Advertise `instance_name` on `port`. The mDNS daemon is spawned internally.
    pub fn new(instance_name: &str, port: u16) -> anyhow::Result<Self> {
        let daemon = ServiceDaemon::new()?;
        let host_name = local_hostname();
        let service_info = ServiceInfo::new(
            SERVICE_TYPE,
            instance_name,
            &host_name,
            (),   // auto-detect all local IPv4 addresses
            port,
            None::<HashMap<String, String>>,
        )?;
        let fullname = service_info.get_fullname().to_string();
        daemon.register(service_info)?;
        Ok(Self { daemon, fullname })
    }

    /// Withdraw the advertisement and shut down the mDNS daemon.
    pub fn stop(self) -> anyhow::Result<()> {
        let _ = self.daemon.unregister(&self.fullname);
        let _ = self.daemon.shutdown();
        Ok(())
    }

    pub fn fullname(&self) -> &str { &self.fullname }
}

// ─── MdnsDiscovery ────────────────────────────────────────────────────────────

/// Discovers NetConduit peers on the LAN via mDNS-SD.
///
/// ```no_run
/// use netconduit_core::mdns::MdnsDiscovery;
/// let disc = MdnsDiscovery::new().unwrap();
/// let peers = disc.scan(std::time::Duration::from_secs(2));
/// for p in peers { println!("{} -> {}:{}", p.fullname, p.addr, p.port); }
/// ```
pub struct MdnsDiscovery {
    daemon: ServiceDaemon,
    /// Accumulated peers visible since last `scan()` or `peers()` call.
    peers:  Arc<Mutex<HashMap<String, DiscoveredPeer>>>,
}

impl MdnsDiscovery {
    /// Start browsing. Events stream in on a background thread.
    pub fn new() -> anyhow::Result<Self> {
        let daemon = ServiceDaemon::new()?;
        let peers: Arc<Mutex<HashMap<String, DiscoveredPeer>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let peers_clone = peers.clone();

        let receiver = daemon.browse(SERVICE_TYPE)?;
        std::thread::spawn(move || {
            while let Ok(event) = receiver.recv() {
                match event {
                    ServiceEvent::ServiceResolved(info) => {
                        let fullname = info.get_fullname().to_string();
                        let port     = info.get_port();
                        let hostname = info.get_hostname().to_string();
                        let props: HashMap<String, String> = info
                            .get_properties()
                            .iter()
                            .map(|p| {
                                (p.key().to_string(),
                                 p.val_str().to_string())
                            })
                            .collect();
                        for addr in info.get_addresses() {
                            let peer = DiscoveredPeer {
                                fullname:   fullname.clone(),
                                addr:       *addr,
                                port,
                                hostname:   hostname.clone(),
                                properties: props.clone(),
                            };
                            peers_clone
                                .lock()
                                .unwrap()
                                .insert(fullname.clone(), peer);
                        }
                    }
                    ServiceEvent::ServiceRemoved(_, fullname) => {
                        peers_clone.lock().unwrap().remove(&fullname);
                    }
                    _ => {}
                }
            }
        });

        Ok(Self { daemon, peers })
    }

    /// Block for `duration`, then return all peers seen so far.
    pub fn scan(&self, duration: Duration) -> Vec<DiscoveredPeer> {
        std::thread::sleep(duration);
        self.peers()
    }

    /// Snapshot of currently known peers (non-blocking).
    pub fn peers(&self) -> Vec<DiscoveredPeer> {
        self.peers.lock().unwrap().values().cloned().collect()
    }

    /// Stop browsing and shut down the daemon.
    pub fn stop(self) -> anyhow::Result<()> {
        let _ = self.daemon.stop_browse(SERVICE_TYPE);
        let _ = self.daemon.shutdown();
        Ok(())
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn local_hostname() -> String {
    hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .map(|h| {
            let h = h.trim().to_string();
            if h.ends_with(".local.")       { h }
            else if h.ends_with(".local")   { format!("{}.", h) }
            else                            { format!("{}.local.", h) }
        })
        .unwrap_or_else(|| "netconduit-host.local.".to_string())
}
