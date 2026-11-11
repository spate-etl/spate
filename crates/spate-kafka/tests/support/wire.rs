//! Kafka wire bytes written by hand: record batches no client would write, the
//! Produce request that carries them to a MockCluster broker, and the framing
//! and field encoding a raw protocol client shares.

use std::io::{self, Read as _, Write as _};
use std::net::TcpStream;

fn zigzag(buf: &mut Vec<u8>, v: i64) {
    let mut z = ((v << 1) ^ (v >> 63)) as u64;
    loop {
        let b = (z & 0x7f) as u8;
        z >>= 7;
        if z == 0 {
            buf.push(b);
            break;
        }
        buf.push(b | 0x80);
    }
}

/// The CRC a [`RawBatch`] carries.
#[derive(Clone, Copy)]
pub(crate) enum BatchCrc {
    /// The bytes as given, which the mock stores unread.
    Literal(u32),
    /// The CRC-32C of the batch's contents.
    Computed,
}

/// A record batch holding one record, written by hand because no client writes
/// the headers these suites need.
#[derive(Clone, Copy)]
pub(crate) struct RawBatch<'a> {
    pub(crate) magic: u8,
    pub(crate) base_offset: i64,
    pub(crate) partition_leader_epoch: i32,
    pub(crate) crc: BatchCrc,
    pub(crate) attributes: i16,
    pub(crate) last_offset_delta: i32,
    pub(crate) timestamp: i64,
    /// The count the mock reserves offsets by, which need not match the one
    /// record the batch holds.
    pub(crate) record_count: i32,
    /// The record's own offset delta, which librdkafka adds to the batch's
    /// base offset unchecked.
    pub(crate) record_offset_delta: i64,
    pub(crate) key: Option<&'a [u8]>,
    pub(crate) value: &'a [u8],
}

impl Default for RawBatch<'_> {
    fn default() -> Self {
        RawBatch {
            magic: 2,
            base_offset: 0,
            partition_leader_epoch: -1,
            crc: BatchCrc::Literal(0),
            attributes: 0,
            last_offset_delta: 0,
            timestamp: 0,
            record_count: 1,
            record_offset_delta: 0,
            key: None,
            value: b"",
        }
    }
}

impl RawBatch<'_> {
    /// The batch as the wire carries it.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut rec = vec![0u8];
        zigzag(&mut rec, 0);
        zigzag(&mut rec, self.record_offset_delta);
        match self.key {
            Some(key) => {
                zigzag(&mut rec, key.len() as i64);
                rec.extend_from_slice(key);
            }
            None => zigzag(&mut rec, -1),
        }
        zigzag(&mut rec, self.value.len() as i64);
        rec.extend_from_slice(self.value);
        zigzag(&mut rec, 0);
        let mut records = Vec::new();
        zigzag(&mut records, rec.len() as i64);
        records.extend_from_slice(&rec);

        let mut tail = Vec::new();
        tail.extend_from_slice(&self.attributes.to_be_bytes());
        tail.extend_from_slice(&self.last_offset_delta.to_be_bytes());
        tail.extend_from_slice(&self.timestamp.to_be_bytes());
        tail.extend_from_slice(&self.timestamp.to_be_bytes());
        tail.extend_from_slice(&(-1i64).to_be_bytes()); // producer id
        tail.extend_from_slice(&(-1i16).to_be_bytes()); // producer epoch
        tail.extend_from_slice(&(-1i32).to_be_bytes()); // base sequence
        tail.extend_from_slice(&self.record_count.to_be_bytes());
        tail.extend_from_slice(&records);

        let crc = match self.crc {
            BatchCrc::Literal(crc) => crc,
            BatchCrc::Computed => crc32c(&tail),
        };
        let mut batch = Vec::new();
        batch.extend_from_slice(&self.base_offset.to_be_bytes());
        batch.extend_from_slice(&((4 + 1 + 4 + tail.len()) as i32).to_be_bytes());
        batch.extend_from_slice(&self.partition_leader_epoch.to_be_bytes());
        batch.push(self.magic);
        batch.extend_from_slice(&crc.to_be_bytes());
        batch.extend_from_slice(&tail);
        batch
    }
}

/// One magic-2 record batch holding one keyed record, with a CRC that does
/// not match its contents.
pub(crate) fn corrupt_batch(key: &[u8], value: &[u8]) -> Vec<u8> {
    RawBatch {
        crc: BatchCrc::Literal(0xdead_beef),
        key: Some(key),
        value,
        ..RawBatch::default()
    }
    .encode()
}

/// How a transaction ended, as its marker records it.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Marker {
    Abort,
    Commit,
}

/// The control batch a broker writes to end a transaction. It takes one offset
/// and a consumer never delivers it.
pub(crate) fn control_batch(marker: Marker) -> Vec<u8> {
    let control_type: i16 = match marker {
        Marker::Abort => 0,
        Marker::Commit => 1,
    };
    let mut key = 0i16.to_be_bytes().to_vec(); // version
    key.extend_from_slice(&control_type.to_be_bytes());
    // Version and coordinator epoch, which a consumer skips.
    let value = [0u8; 6];
    RawBatch {
        attributes: ATTR_TRANSACTIONAL | ATTR_CONTROL,
        crc: BatchCrc::Computed,
        key: Some(&key),
        value: &value,
        ..RawBatch::default()
    }
    .encode()
}

const ATTR_TRANSACTIONAL: i16 = 0x10;
const ATTR_CONTROL: i16 = 0x20;

/// Write `marker` to `partition` of `topic`.
pub(crate) fn write_marker(broker: &str, topic: &str, partition: i32, marker: Marker) {
    assert_eq!(
        produce_batch(broker, topic, partition, &control_batch(marker)),
        0
    );
}

/// [`try_produce_batch`] on a broker the test knows is up.
pub(crate) fn produce_batch(broker: &str, topic: &str, partition: i32, batch: &[u8]) -> i16 {
    try_produce_batch(broker, topic, partition, batch).unwrap()
}

/// Send `batch` to `partition` of `topic` as a version 3 Produce request
/// and return the partition's error code. A client library would compute
/// the batch's CRC itself; the mock stores the batch as sent.
///
/// # Errors
///
/// Any failure of the connection, which a mock broker back from an outage gives
/// until its listener has room.
pub(crate) fn try_produce_batch(
    broker: &str,
    topic: &str,
    partition: i32,
    batch: &[u8],
) -> io::Result<i16> {
    let mut b = Vec::new();
    b.extend_from_slice(&(-1i16).to_be_bytes());
    b.extend_from_slice(&1i16.to_be_bytes());
    b.extend_from_slice(&10_000i32.to_be_bytes());
    b.extend_from_slice(&1i32.to_be_bytes());
    str16(&mut b, topic);
    b.extend_from_slice(&1i32.to_be_bytes());
    b.extend_from_slice(&partition.to_be_bytes());
    bytes32(&mut b, batch);
    let resp = request(&mut TcpStream::connect(broker)?, 0, 3, &b)?;
    let at = 4 + 2 + topic.len() + 4 + 4;
    Ok(i16::from_be_bytes([resp[at], resp[at + 1]]))
}

/// Append `s` as a protocol `STRING`: an `i16` length, then the bytes.
pub(crate) fn str16(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&i16::try_from(s.len()).unwrap().to_be_bytes());
    buf.extend_from_slice(s.as_bytes());
}

/// Append `b` as protocol `BYTES`: an `i32` length, then the bytes.
pub(crate) fn bytes32(buf: &mut Vec<u8>, b: &[u8]) {
    buf.extend_from_slice(&i32::try_from(b.len()).unwrap().to_be_bytes());
    buf.extend_from_slice(b);
}

/// Send one request for `api` at `version` with `body` over `s`, and return the
/// response after its correlation id.
///
/// # Errors
///
/// Any failure of the connection.
pub(crate) fn request(
    s: &mut TcpStream,
    api: i16,
    version: i16,
    body: &[u8],
) -> io::Result<Vec<u8>> {
    let mut b = Vec::new();
    b.extend_from_slice(&api.to_be_bytes());
    b.extend_from_slice(&version.to_be_bytes());
    b.extend_from_slice(&7i32.to_be_bytes());
    str16(&mut b, "spate");
    b.extend_from_slice(body);
    s.write_all(&i32::try_from(b.len()).unwrap().to_be_bytes())?;
    s.write_all(&b)?;
    let mut len = [0u8; 4];
    s.read_exact(&mut len)?;
    let mut resp = vec![0u8; usize::try_from(i32::from_be_bytes(len)).unwrap()];
    s.read_exact(&mut resp)?;
    resp.drain(..4);
    Ok(resp)
}

/// Reads protocol fields from the front of a response.
pub(crate) struct Reader<'a>(pub(crate) &'a [u8]);

impl Reader<'_> {
    pub(crate) fn i16(&mut self) -> i16 {
        let v = i16::from_be_bytes(self.0[..2].try_into().unwrap());
        self.0 = &self.0[2..];
        v
    }

    pub(crate) fn i32(&mut self) -> i32 {
        let v = i32::from_be_bytes(self.0[..4].try_into().unwrap());
        self.0 = &self.0[4..];
        v
    }

    pub(crate) fn string(&mut self) -> String {
        let n = usize::try_from(self.i16()).unwrap();
        let v = String::from_utf8(self.0[..n].to_vec()).unwrap();
        self.0 = &self.0[n..];
        v
    }

    pub(crate) fn skip_bytes(&mut self) {
        let n = usize::try_from(self.i32()).unwrap();
        self.0 = &self.0[n..];
    }
}

/// CRC-32 (IEEE), the checksum of a magic-0/1 message.
fn crc32_ieee(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &b in data {
        crc ^= u32::from(b);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

/// CRC-32C, the checksum of a magic-2 batch.
fn crc32c(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &b in data {
        crc ^= u32::from(b);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82f6_3b78 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

/// A valid magic-2 batch holding one record, whose header reserves
/// `reserve` offsets in the mock's log.
pub(crate) fn v2_batch(key: &[u8], value: &[u8], reserve: i32) -> Vec<u8> {
    RawBatch {
        partition_leader_epoch: 0,
        crc: BatchCrc::Computed,
        timestamp: 1,
        record_count: reserve,
        key: Some(key),
        value,
        ..RawBatch::default()
    }
    .encode()
}

/// One magic-1 message at `offset`; `corrupt` gives it a CRC that does
/// not match its contents.
pub(crate) fn v1_message(offset: i64, key: &[u8], value: &[u8], corrupt: bool) -> Vec<u8> {
    let mut body = vec![1u8, 0u8];
    body.extend_from_slice(&1i64.to_be_bytes());
    body.extend_from_slice(&(key.len() as i32).to_be_bytes());
    body.extend_from_slice(key);
    body.extend_from_slice(&(value.len() as i32).to_be_bytes());
    body.extend_from_slice(value);
    let crc = crc32_ieee(&body) ^ u32::from(corrupt);
    let mut msg = Vec::new();
    msg.extend_from_slice(&offset.to_be_bytes());
    msg.extend_from_slice(&((4 + body.len()) as i32).to_be_bytes());
    msg.extend_from_slice(&crc.to_be_bytes());
    msg.extend_from_slice(&body);
    msg
}
