//! Networking code for sending and receiving fixed size `Vec<u8>` between machines.

use std::io;
use std::io::Result;
use std::sync::Arc;
use std::time::Duration;

use byteorder::{ReadBytesExt, WriteBytesExt};
use columnar::Columnar;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::sleep;

// This constant is sent along immediately after establishing a TCP stream, so
// that it is easy to sniff out Timely traffic when it is multiplexed with
// other traffic on the same port.
const HANDSHAKE_MAGIC: u64 = 0xc2f1fb770118add9;

/// The byte order for writing message headers and stream initialization.
type ByteOrder = byteorder::BigEndian;

/// Framing data for each `Vec<u8>` transmission, indicating a typed channel, the source and
/// destination workers, and the length in bytes.
// *Warning*: Adding, removing and altering fields requires to adjust the implementation below!
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize, Columnar)]
pub struct MessageHeader {
    /// index of channel.
    pub channel:    usize,
    /// index of worker sending message.
    pub source:     usize,
    /// lower bound of index of worker receiving message.
    pub target_lower:     usize,
    /// upper bound of index of worker receiving message.
    ///
    /// This is often `self.target_lower + 1` for point to point messages,
    /// but can be larger for broadcast messages.
    pub target_upper:     usize,
    /// number of bytes in message.
    pub length:     usize,
    /// sequence number.
    pub seqno:      usize,
}

impl MessageHeader {

    /// The number of `usize` fields in [MessageHeader].
    const FIELDS: usize = 6;

    /// Returns a header when there is enough supporting data
    #[inline]
    pub fn try_read(bytes: &[u8]) -> Option<MessageHeader> {
        let mut cursor = io::Cursor::new(bytes);
        let mut buffer = [0; Self::FIELDS];
        cursor.read_u64_into::<ByteOrder>(&mut buffer).ok()?;
        let header = MessageHeader {
            // Order must match writing order.
            channel: buffer[0] as usize,
            source: buffer[1] as usize,
            target_lower: buffer[2] as usize,
            target_upper: buffer[3] as usize,
            length: buffer[4] as usize,
            seqno: buffer[5] as usize,
        };

        if bytes.len() >= header.required_bytes() {
            Some(header)
        } else {
            None
        }
    }

    /// Writes the header as binary data.
    #[inline]
    pub fn write_to<W: ::std::io::Write>(&self, writer: &mut W) -> Result<()> {
        let mut buffer = [0u8; std::mem::size_of::<u64>() * Self::FIELDS];
        let mut cursor = io::Cursor::new(&mut buffer[..]);
        // Order must match reading order.
        WriteBytesExt::write_u64::<ByteOrder>(&mut cursor, self.channel as u64)?;
        WriteBytesExt::write_u64::<ByteOrder>(&mut cursor, self.source as u64)?;
        WriteBytesExt::write_u64::<ByteOrder>(&mut cursor, self.target_lower as u64)?;
        WriteBytesExt::write_u64::<ByteOrder>(&mut cursor, self.target_upper as u64)?;
        WriteBytesExt::write_u64::<ByteOrder>(&mut cursor, self.length as u64)?;
        WriteBytesExt::write_u64::<ByteOrder>(&mut cursor, self.seqno as u64)?;

        writer.write_all(&buffer[..])
    }

    /// The number of bytes required for the header and data.
    #[inline]
    pub fn required_bytes(&self) -> usize {
        self.header_bytes() + self.length
    }

    /// The number of bytes required for the header.
    #[inline(always)]
    pub fn header_bytes(&self) -> usize {
        std::mem::size_of::<u64>() * Self::FIELDS
    }
}

/// Creates socket connections from a list of host addresses.
///
/// The item at index `i` in the resulting vec, is a `Some(TcpSocket)` to process `i`, except
/// for item `my_index` which is `None` (no socket to self).
pub async fn create_sockets(addresses: Vec<String>, my_index: usize, noisy: bool) -> Result<Vec<Option<TcpStream>>> {

    let hosts1 = Arc::new(addresses);
    let hosts2 = Arc::clone(&hosts1);

    let start_task = tokio::task::spawn_local(start_connections(hosts1, my_index, noisy));
    let await_task = tokio::task::spawn_local(await_connections(hosts2, my_index, noisy));

    let mut results = start_task.await.unwrap()?;
    results.push(None);
    let to_extend = await_task.await.unwrap()?;
    results.extend(to_extend);

    if noisy { println!("worker {}:\tinitialization complete", my_index) }

    Ok(results)
}


/// Result contains connections `[0, my_index - 1]`.
pub async fn start_connections(addresses: Arc<Vec<String>>, my_index: usize, noisy: bool) -> Result<Vec<Option<TcpStream>>> {
    let mut results = Vec::new();
    for (index, address) in addresses.iter().take(my_index).enumerate() {
        let res = loop {
            match TcpStream::connect(address).await {
                Ok(mut stream) => {
                    stream.set_nodelay(true).expect("set_nodelay call failed");
                    stream.write_u64(HANDSHAKE_MAGIC).await.expect("failed to encode/send handshake magic");
                    stream.write_u64(my_index as u64).await.expect("failed to encode/send worker index");
                    if noisy { println!("worker {}:\tconnection to worker {}", my_index, index); }
                    break Some(stream);
                },
                Err(error) => {
                    println!("worker {}:\terror connecting to worker {}: {}; retrying", my_index, index, error);
                    sleep(Duration::from_secs(1)).await;
                },
            }
        };
        results.push(res);
    }

    Ok(results)
}

/// Result contains connections `[my_index + 1, addresses.len() - 1]`.
pub async fn await_connections(addresses: Arc<Vec<String>>, my_index: usize, noisy: bool) -> Result<Vec<Option<TcpStream>>> {
    let mut results: Vec<_> = (0..(addresses.len() - my_index - 1)).map(|_| None).collect();
    let listener = TcpListener::bind(&addresses[my_index][..]).await?;

    for _ in (my_index + 1) .. addresses.len() {
        let mut stream = listener.accept().await?.0;
        stream.set_nodelay(true).expect("set_nodelay call failed");
        let mut buffer = [0u8;16];
        stream.read_exact(&mut buffer).await?;
        let mut cursor = io::Cursor::new(buffer);
        let magic = ReadBytesExt::read_u64::<ByteOrder>(&mut cursor).expect("failed to decode magic");
        if magic != HANDSHAKE_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData,
                "received incorrect timely handshake"));
        }
        let identifier = ReadBytesExt::read_u64::<ByteOrder>(&mut cursor).expect("failed to decode worker index") as usize;
        results[identifier - my_index - 1] = Some(stream);
        if noisy { println!("worker {}:\tconnection from worker {}", my_index, identifier); }
    }

    Ok(results)
}
