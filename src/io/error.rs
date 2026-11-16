use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    RangeOutOfBounds,
    InvalidArgument(&'static str),
    /// Another cache, in this process or another, owns a cache file.
    Busy,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::RangeOutOfBounds => write!(f, "requested range is out of bounds"),
            Self::InvalidArgument(msg) => write!(f, "invalid argument: {msg}"),
            Self::Busy => write!(f, "a cache file is owned by another cache"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
