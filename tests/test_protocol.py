"""
Tests for the Protocol Layer

Tests encoding/decoding, header parsing, and message round-trips.
"""

import pytest
import struct
import time

from conduit.protocol import (
    MAGIC,
    HEADER_SIZE,
    PROTOCOL_VERSION,
    MessageType,
    MessageFlags,
    MessageHeader,
    ProtocolEncoder,
    ProtocolDecoder,
    DecodedMessage,
    DecodeError,
    IncompleteMessageError,
)


class TestMessageHeader:
    """Tests for MessageHeader class."""
    
    def test_header_size(self):
        """Verify header is exactly 32 bytes."""
        assert HEADER_SIZE == 32
        assert struct.calcsize(MessageHeader.STRUCT_FORMAT) == 32
    
    def test_create_header(self):
        """Test creating a new header."""
        header = MessageHeader.create(
            message_type=MessageType.MESSAGE,
            content_length=100,
            correlation_id=12345,
        )
        
        assert header.magic == MAGIC
        assert header.version == PROTOCOL_VERSION
        assert header.message_type == MessageType.MESSAGE
        assert header.content_length == 100
        assert header.correlation_id == 12345
        assert header.timestamp > 0
    
    def test_header_round_trip(self):
        """Test serializing and deserializing header."""
        original = MessageHeader.create(
            message_type=MessageType.RPC_REQUEST,
            content_length=256,
            correlation_id=99999,
            flags=MessageFlags.REQUIRE_ACK,
        )
        
        # Serialize
        data = original.to_bytes()
        assert len(data) == HEADER_SIZE
        
        # Deserialize
        restored = MessageHeader.from_bytes(data)
        
        assert restored.magic == original.magic
        assert restored.version == original.version
        assert restored.message_type == original.message_type
        assert restored.flags == original.flags
        assert restored.content_length == original.content_length
        assert restored.correlation_id == original.correlation_id
        assert restored.timestamp == original.timestamp
    
    def test_header_validation(self):
        """Test header validation."""
        header = MessageHeader.create(
            message_type=MessageType.MESSAGE,
            content_length=100,
        )
        
        # Valid header should not raise
        header.validate()
        
        # Invalid magic should raise
        header.magic = b'XXXX'
        with pytest.raises(ValueError, match="Invalid magic"):
            header.validate()
    
    def test_invalid_magic_on_deserialize(self):
        """Test that invalid magic bytes raise error."""
        bad_data = b'XXXX' + b'\x00' * 28
        with pytest.raises(ValueError, match="Invalid magic"):
            MessageHeader.from_bytes(bad_data)
    
    def test_short_data_raises_error(self):
        """Test that short data raises error."""
        with pytest.raises(ValueError, match="Header too short"):
            MessageHeader.from_bytes(b'CNDT' + b'\x00' * 10)
    
    def test_is_control_message(self):
        """Test control message detection."""
        ping_header = MessageHeader.create(
            message_type=MessageType.HEARTBEAT_PING,
            content_length=0,
        )
        assert ping_header.is_control_message()
        
        msg_header = MessageHeader.create(
            message_type=MessageType.MESSAGE,
            content_length=100,
        )
        assert not msg_header.is_control_message()
    
    def test_is_rpc(self):
        """Test RPC message detection."""
        rpc_header = MessageHeader.create(
            message_type=MessageType.RPC_REQUEST,
            content_length=50,
        )
        assert rpc_header.is_rpc()
        
        msg_header = MessageHeader.create(
            message_type=MessageType.MESSAGE,
            content_length=50,
        )
        assert not msg_header.is_rpc()


class TestProtocolEncoder:
    """Tests for ProtocolEncoder class."""
    
    def test_encode_simple_message(self):
        """Test encoding a simple message."""
        encoder = ProtocolEncoder()
        
        data = encoder.encode_message("hello", {"text": "world"})
        
        assert len(data) > 0
        decoded = ProtocolDecoder.decode_single(data)
        assert decoded.get_message_type_str() == "hello"
        assert decoded.get_data() == {"text": "world"}
    
    def test_encode_rpc_request(self):
        """Test encoding RPC request."""
        encoder = ProtocolEncoder()
        
        data, corr_id = encoder.encode_rpc_request(
            method="calculate",
            params={"a": 10, "b": 20},
        )
        
        assert len(data) > 0
        assert corr_id > 0
        
        # Correlation ID should increment
        data2, corr_id2 = encoder.encode_rpc_request("test")
        assert corr_id2 == corr_id + 1
    
    def test_encode_rpc_response(self):
        """Test encoding RPC response."""
        encoder = ProtocolEncoder()
        
        data = encoder.encode_rpc_response(
            result={"value": 42},
            correlation_id=123,
            success=True,
        )
        
        assert len(data) > 0
    
    def test_encode_control_messages(self):
        """Test encoding control messages."""
        encoder = ProtocolEncoder()
        
        # Heartbeat
        ping = encoder.encode_heartbeat_ping()
        pong = encoder.encode_heartbeat_pong()
        assert len(ping) > 0
        assert len(pong) > 0
        
        # Flow control
        pause = encoder.encode_pause()
        resume = encoder.encode_resume()
        assert len(pause) > 0
        assert len(resume) > 0
        
        # Close
        close = encoder.encode_close()
        close_ack = encoder.encode_close_ack()
        assert len(close) > 0
        assert len(close_ack) > 0
    
    def test_encode_auth_messages(self):
        """Test encoding auth messages."""
        encoder = ProtocolEncoder()
        
        auth_req = encoder.encode_auth_request(
            password_hash="hash123",
            client_info={"name": "test_client"},
        )
        assert len(auth_req) > 0
        
        auth_success = encoder.encode_auth_success(
            session_token="token456",
            server_info={"name": "test_server"},
        )
        assert len(auth_success) > 0


class TestProtocolDecoder:
    """Tests for ProtocolDecoder class."""
    
    def test_decode_simple_message(self):
        """Test decoding a simple message."""
        encoder = ProtocolEncoder()
        decoder = ProtocolDecoder()
        
        # Encode
        original_data = {"text": "hello world"}
        encoded = encoder.encode_message("greeting", original_data)
        
        # Decode
        decoder.feed(encoded)
        message = decoder.decode_one()
        
        assert message is not None
        assert message.message_type == MessageType.MESSAGE
        assert message.get_message_type_str() == "greeting"
        assert message.get_data() == original_data
    
    def test_decode_rpc_request(self):
        """Test decoding RPC request."""
        encoder = ProtocolEncoder()
        decoder = ProtocolDecoder()
        
        encoded, corr_id = encoder.encode_rpc_request(
            method="test_method",
            params={"arg1": "value1"},
        )
        
        decoder.feed(encoded)
        message = decoder.decode_one()
        
        assert message is not None
        assert message.message_type == MessageType.RPC_REQUEST
        assert message.correlation_id == corr_id
        assert message.get_rpc_method() == "test_method"
        assert message.get_rpc_params() == {"arg1": "value1"}
    
    def test_decode_rpc_response(self):
        """Test decoding RPC response."""
        encoder = ProtocolEncoder()
        decoder = ProtocolDecoder()
        
        result = {"answer": 42}
        encoded = encoder.encode_rpc_response(result, correlation_id=999, success=True)
        
        decoder.feed(encoded)
        message = decoder.decode_one()
        
        assert message is not None
        assert message.message_type == MessageType.RPC_RESPONSE
        assert message.correlation_id == 999
        assert message.is_success()
        assert message.get_rpc_result() == result
    
    def test_decode_rpc_error(self):
        """Test decoding RPC error."""
        encoder = ProtocolEncoder()
        decoder = ProtocolDecoder()
        
        encoded = encoder.encode_rpc_error(
            error_message="Something went wrong",
            correlation_id=888,
            error_code=500,
        )
        
        decoder.feed(encoded)
        message = decoder.decode_one()
        
        assert message is not None
        assert message.message_type == MessageType.RPC_ERROR
        assert message.correlation_id == 888
        assert not message.is_success()
        assert message.get_rpc_error() == "Something went wrong"
    
    def test_decode_multiple_messages(self):
        """Test decoding multiple messages in sequence."""
        encoder = ProtocolEncoder()
        decoder = ProtocolDecoder()
        
        # Encode multiple messages
        msg1 = encoder.encode_message("type1", {"data": 1})
        msg2 = encoder.encode_message("type2", {"data": 2})
        msg3 = encoder.encode_message("type3", {"data": 3})
        
        # Feed all at once
        decoder.feed(msg1 + msg2 + msg3)
        
        # Decode all
        messages = decoder.decode_all()
        
        assert len(messages) == 3
        assert messages[0].get_message_type_str() == "type1"
        assert messages[1].get_message_type_str() == "type2"
        assert messages[2].get_message_type_str() == "type3"
    
    def test_decode_partial_message(self):
        """Test decoding with partial data."""
        encoder = ProtocolEncoder()
        decoder = ProtocolDecoder()
        
        encoded = encoder.encode_message("test", {"large": "data" * 100})
        
        # Feed partial data
        decoder.feed(encoded[:20])
        assert decoder.decode_one() is None
        
        # Feed more
        decoder.feed(encoded[20:50])
        assert decoder.decode_one() is None
        
        # Feed rest
        decoder.feed(encoded[50:])
        message = decoder.decode_one()
        
        assert message is not None
        assert message.get_message_type_str() == "test"
    
    def test_decode_single_static(self):
        """Test static single message decode."""
        encoder = ProtocolEncoder()
        
        encoded = encoder.encode_message("test", {"key": "value"})
        message = ProtocolDecoder.decode_single(encoded)
        
        assert message.get_message_type_str() == "test"
    
    def test_decode_single_incomplete_raises(self):
        """Test that incomplete data raises error."""
        with pytest.raises(IncompleteMessageError):
            ProtocolDecoder.decode_single(b'CNDT' + b'\x00' * 10)
    
    def test_buffer_management(self):
        """Test buffer size and clearing."""
        decoder = ProtocolDecoder()
        
        assert decoder.buffer_size() == 0
        
        decoder.feed(b'some data')
        assert decoder.buffer_size() == 9
        
        decoder.clear()
        assert decoder.buffer_size() == 0


class TestRoundTrip:
    """End-to-end round-trip tests."""
    
    def test_message_round_trip(self):
        """Test complete message encode/decode cycle."""
        encoder = ProtocolEncoder()
        decoder = ProtocolDecoder()
        
        test_cases = [
            ("simple", "hello"),
            ("dict", {"key": "value", "number": 42}),
            ("list", [1, 2, 3, "four"]),
            ("nested", {"outer": {"inner": [1, 2, 3]}}),
            ("unicode", {"text": "Hello 世界 🌍"}),
            ("binary", {"data": b"binary data".hex()}),
        ]
        
        for msg_type, data in test_cases:
            encoded = encoder.encode_message(msg_type, data)
            decoder.feed(encoded)
            message = decoder.decode_one()
            
            assert message is not None, f"Failed for {msg_type}"
            assert message.get_message_type_str() == msg_type
            assert message.get_data() == data
    
    def test_rpc_round_trip(self):
        """Test complete RPC encode/decode cycle."""
        encoder = ProtocolEncoder()
        decoder = ProtocolDecoder()
        
        # Request
        req_encoded, corr_id = encoder.encode_rpc_request(
            method="add",
            params={"a": 10, "b": 20},
        )
        decoder.feed(req_encoded)
        req_msg = decoder.decode_one()
        
        assert req_msg.get_rpc_method() == "add"
        assert req_msg.get_rpc_params() == {"a": 10, "b": 20}
        
        # Response
        resp_encoded = encoder.encode_rpc_response(
            result={"sum": 30},
            correlation_id=corr_id,
        )
        decoder.feed(resp_encoded)
        resp_msg = decoder.decode_one()
        
        assert resp_msg.correlation_id == corr_id
        assert resp_msg.is_success()
        assert resp_msg.get_rpc_result() == {"sum": 30}


class TestCompression:
    """Tests for compression support."""
    
    def test_compression_enabled(self):
        """Test that compression works when enabled."""
        encoder = ProtocolEncoder(enable_compression=True)
        decoder = ProtocolDecoder()
        
        # Create large payload
        large_data = {"data": "x" * 10000}
        encoded = encoder.encode_message("large", large_data)
        
        # Decode
        decoder.feed(encoded)
        message = decoder.decode_one()
        
        assert message is not None
        assert message.get_data() == large_data
        
        # Verify it was compressed (header should have flag set)
        # and encoded size should be smaller than uncompressed
    
    def test_small_messages_not_compressed(self):
        """Test that small messages are not compressed."""
        encoder = ProtocolEncoder(enable_compression=True)
        decoder = ProtocolDecoder()
        
        small_data = {"data": "small"}
        encoded = encoder.encode_message("small", small_data)
        
        decoder.feed(encoded)
        message = decoder.decode_one()
        
        assert message is not None
        assert not message.is_compressed()


class TestOrderingAndByteOrder:
    """Tests for message ordering, reordering buffer, and byte-order markers."""
    
    def test_byte_order_helpers(self):
        """Test byte order helper functions in netconduit_core."""
        from netconduit import host_byte_order, pack_u64, pack_u32, unpack_u64, unpack_u32
        
        # Test host_byte_order returns 0 or 1
        hbo = host_byte_order()
        assert hbo in (0, 1)
        
        # Test pack/unpack u32
        val_u32 = 0x12345678
        packed_be = pack_u32(val_u32, 0)
        packed_le = pack_u32(val_u32, 1)
        assert packed_be == b"\x12\x34\x56\x78"
        assert packed_le == b"\x78\x56\x34\x12"
        
        assert unpack_u32(packed_be, 0) == val_u32
        assert unpack_u32(packed_le, 1) == val_u32
        
        # Test pack/unpack u64
        val_u64 = 0x123456789ABCDEF0
        packed_be64 = pack_u64(val_u64, 0)
        packed_le64 = pack_u64(val_u64, 1)
        assert packed_be64 == b"\x12\x34\x56\x78\x9A\xBC\xDE\xF0"
        assert packed_le64 == b"\xF0\xDE\xBC\x9A\x78\x56\x34\x12"
        
        assert unpack_u64(packed_be64, 0) == val_u64
        assert unpack_u64(packed_le64, 1) == val_u64

    def test_sequence_counter(self):
        """Test SequenceCounter from netconduit_core."""
        from netconduit import SequenceCounter
        
        sc = SequenceCounter()
        assert sc.peek(1) == 0
        assert sc.next(1) == 0
        assert sc.peek(1) == 1
        assert sc.next(1) == 1
        assert sc.next(1) == 2
        assert sc.next(2) == 0
        sc.reset_stream(1)
        assert sc.peek(1) == 0
        assert sc.peek(2) == 1
        sc.reset_all()
        assert sc.peek(2) == 0

    def test_reorder_buffer(self):
        """Test ReorderBuffer from netconduit_core."""
        from netconduit import ReorderBuffer
        
        rb = ReorderBuffer(max_gap=3, max_buf=10)
        stream_id = 42
        assert rb.next_expected(stream_id) == 0
        assert rb.buffered_count(stream_id) == 0
        
        # Push next expected
        assert rb.push(stream_id, 0, b"packet0")
        assert rb.buffered_count(stream_id) == 1
        
        # Drain ready
        ready = rb.drain_ready(stream_id)
        assert len(ready) == 1
        assert ready[0] == (0, b"packet0")
        assert rb.next_expected(stream_id) == 1
        assert rb.buffered_count(stream_id) == 0
        
        # Push out of order
        assert rb.push(stream_id, 2, b"packet2")
        assert rb.push(stream_id, 1, b"packet1")
        assert rb.buffered_count(stream_id) == 2
        
        # Drain both
        ready = rb.drain_ready(stream_id)
        assert len(ready) == 2
        assert ready[0] == (1, b"packet1")
        assert ready[1] == (2, b"packet2")
        assert rb.next_expected(stream_id) == 3
        
        # Push duplicate/old
        assert rb.push(stream_id, 2, b"old")
        
        # Push with gap within max_gap (expecting 3, pushed 5)
        assert rb.push(stream_id, 5, b"packet5")
        ready = rb.drain_ready(stream_id)
        assert len(ready) == 0
        
        # Clean up stream for gap skip test
        rb.reset_stream(stream_id)
        assert rb.next_expected(stream_id) == 0
        
        # Set expected to 3 by pushing and draining 0, 1, 2
        assert rb.push(stream_id, 0, b"p0")
        assert rb.push(stream_id, 1, b"p1")
        assert rb.push(stream_id, 2, b"p2")
        rb.drain_ready(stream_id)
        assert rb.next_expected(stream_id) == 3
        
        # Push exceeding max_gap (expecting 3, got 7, gap is 7 - 3 = 4 > 3)
        assert rb.push(stream_id, 7, b"packet7")
        ready = rb.drain_ready(stream_id)
        assert len(ready) == 1
        assert ready[0] == (7, b"packet7")
        assert rb.next_expected(stream_id) == 8


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
