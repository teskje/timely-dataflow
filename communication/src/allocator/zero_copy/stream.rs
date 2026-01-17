//! Abstractions over network streams.

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{tcp, TcpStream};
#[cfg(unix)]
use tokio::net::{unix, UnixStream};

/// An abstraction over network streams.
pub trait Stream: Sized + Send + Sync + AsyncRead + AsyncWrite + Unpin {
    /// Stream read half.
    type ReadHalf: Sized + Send + Sync + AsyncRead + Unpin;
    /// Stream write half.
    type WriteHalf: Sized + Send + Sync + AsyncWrite + Unpin;

    /// Split the stream into read half and write half.
    fn split(self) -> (Self::ReadHalf, Self::WriteHalf);
}

impl Stream for TcpStream {
    type ReadHalf = tcp::OwnedReadHalf;
    type WriteHalf = tcp::OwnedWriteHalf;

    fn split(self) -> (Self::ReadHalf, Self::WriteHalf) {
        self.into_split()
    }
}

#[cfg(unix)]
impl Stream for UnixStream {
    type ReadHalf = unix::OwnedReadHalf;
    type WriteHalf = unix::OwnedWriteHalf;

    fn split(self) -> (Self::ReadHalf, Self::WriteHalf) {
        self.into_split()
    }
}
