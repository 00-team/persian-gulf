use actix_web::HttpResponse;

pub type Horp = Result<HttpResponse, AppErr>;
// pub type Jorp<T> = Result<Json<T>, AppErr>;

use actix_web::{ResponseError, body::BoxBody, http::StatusCode};
// use awc::error::SendRequestError;
// use sabad::models::SeedError;
use tokio::task::JoinError;

#[derive(Debug, Default, serde::Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
/// all the api error codes
pub enum ErrorCode {
    #[default]
    Unknown = 0,
    BadRequest,
    Forbidden,
    BadAuth,
    NotFound,
    ServerError,
    DatabaseError,
}

impl ErrorCode {
    fn status(&self) -> u16 {
        match self {
            Self::BadRequest => 400,

            Self::Forbidden => 403,
            Self::BadAuth => 403,

            Self::NotFound => 404,

            Self::Unknown => 500,
            Self::ServerError | Self::DatabaseError => 500,
        }
    }
}

impl From<ErrorCode> for AppErr {
    fn from(value: ErrorCode) -> Self {
        Self { status: value.status(), debug: None, code: value }
    }
}

impl From<JoinError> for AppErr {
    fn from(value: JoinError) -> Self {
        Self::from(ErrorCode::ServerError)
            .debug(&format!("failed to join tokio task: {value:#?}"))
    }
}

#[derive(serde::Serialize, Debug, Clone)]
pub struct AppErr {
    pub status: u16,
    pub code: ErrorCode,
    pub debug: Option<String>,
}

impl AppErr {
    // pub fn new(status: u16, code: SeedError) -> Self {
    //     Self { status, code, debug: None }
    // }

    pub fn server_error() -> Self {
        Self { status: 500, code: ErrorCode::ServerError, debug: None }
    }

    pub fn debug(mut self, debug: &str) -> Self {
        self.debug = Some(debug.to_string());
        self
    }

    // pub fn code<C: Into<u16>>(mut self, code: C) -> Self {
    //     self.code = code.into();
    //     self
    // }
}

impl std::error::Error for AppErr {}

impl std::fmt::Display for AppErr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl ResponseError for AppErr {
    fn status_code(&self) -> StatusCode {
        StatusCode::from_u16(self.status)
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponse::build(self.status_code()).json(self)
    }
}

// macro_rules! from_own_ref {
//     ($from:path, $to:path) => {
//         impl From<$from> for $to {
//             fn from(value: $from) -> Self {
//                 Self::from(&value)
//             }
//         }
//     };
// }

// macro_rules! impl_from_err {
//     ($ty:path) => {
//         impl From<$ty> for AppErr {
//             fn from(value: $ty) -> Self {
//                 let content = value.to_string();
//                 log::error!("{}: {content} | {value:?}", stringify!($ty));
//                 Self {
//                     status: 500,
//                     code: SeedError::ServerError,
//                     debug: Some(format!("{}: {}", stringify!($ty), content)),
//                 }
//             }
//         }
//     };
// }

impl From<actix_web::http::header::ToStrError> for AppErr {
    fn from(value: actix_web::http::header::ToStrError) -> Self {
        // use actix_web::http::header::ToStrError
        Self {
            status: 400,
            code: ErrorCode::BadRequest,
            debug: Some(format!(
                "could not convert this header value to string: {value:?}"
            )),
        }
    }
}

macro_rules! fse {
    ($ty:path) => {
        impl From<$ty> for AppErr {
            fn from(value: $ty) -> Self {
                log::error!("_500_: [{}] {value:?}", stringify!($ty));
                Self::server_error().debug(stringify!($ty))
            }
        }
    };
}

// fse!(reqwest::Error);
fse!(std::io::Error);
// fse!(serde_json::Error);
// fse!(mapack::protobuf::Error);

// impl_from_err!(io::Error);
// impl_from_err!(SendRequestError);

#[macro_export]
macro_rules! err {
    ($code:ident) => {
        Err($crate::AppErr::from($crate::ErrorCode::$code))
    };

    ($code:ident, $debug:literal) => {
        Err($crate::AppErr::from($crate::ErrorCode::$code).debug($debug))
    };

    ($code:ident, $debug:expr) => {
        Err($crate::AppErr::from($crate::ErrorCode::$code).debug(&$debug))
    };

    (r, $code:ident) => {
        $crate::AppErr::from($crate::ErrorCode::$code)
    };

    (c, $code:ident) => {
        Err($crate::ErrorCode::$code)
    };

    (r,$code:ident, $debug:literal) => {
        $crate::AppErr::from($crate::ErrorCode::$code).debug($debug)
    };

    (r,$code:ident, $debug:expr) => {
        $crate::AppErr::from($crate::ErrorCode::$code).debug(&$debug)
    };
}
