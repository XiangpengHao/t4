use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Format(String),
    InputError(proof_core::input_kv::InputError),
    RangeOutOfBounds,
    NotFound,
    LockPoisoned,
    InvalidArgument(&'static str),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Format(msg) => write!(f, "format error: {msg}"),
            Self::InputError(err) => write!(f, "input error: {:?}", err),
            Self::RangeOutOfBounds => write!(f, "requested range is out of bounds"),
            Self::NotFound => write!(f, "key not found"),
            Self::LockPoisoned => write!(f, "internal lock poisoned"),
            Self::InvalidArgument(msg) => write!(f, "invalid argument: {msg}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<proof_core::wal::WalEntryDecodeError> for Error {
    fn from(value: proof_core::wal::WalEntryDecodeError) -> Self {
        Self::Format(format!("wal entry decode error: {:?}", value))
    }
}

impl From<proof_core::input_kv::InputError> for Error {
    fn from(value: proof_core::input_kv::InputError) -> Self {
        Self::InputError(value)
    }
}
