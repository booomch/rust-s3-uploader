use std::str::FromStr;
use axum::{
    body::{Bytes, Full, StreamBody},
    extract::{ContentLengthLimit, Extension, Multipart, Path},
    http::StatusCode,
    response::{Headers, IntoResponse},
    routing::{get, post},
    AddExtensionLayer, Json, Router,
};
use rusoto_core::{request::BufferedHttpResponse, RusotoError, Region};
use rusoto_credential::{EnvironmentProvider, ProvideAwsCredentials};
use rusoto_s3::{
    GetObjectError, GetObjectRequest, HeadObjectRequest, PutObjectRequest, S3Client, S3,
};
use std::{convert::Infallible, ffi::OsStr, net::SocketAddr};

#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate lazy_static;

fn ensure_var<K: AsRef<OsStr>>(key: K) -> anyhow::Result<String> {
    std::env::var(&key).map_err(|e| anyhow::anyhow!("{}: {:?}", e, key.as_ref()))
}

lazy_static! {
    static ref ADDRESS: SocketAddr = 
        format!("127.0.0.1:{}",ensure_var("PORT").unwrap()).parse().unwrap();
    static ref URL: String = ensure_var("URL").unwrap();
    static ref S3_BUCKET: String = ensure_var("S3_BUCKET").unwrap();
    static ref S3_REGION: String = ensure_var("S3_REGION").unwrap();
}

fn generate_id() -> String {
    std::iter::repeat_with(fastrand::alphanumeric)
        .take(7)
        .collect()
}

async fn generate_unique_id(s3: &S3Client) -> anyhow::Result<String> {
    loop {
        let id = generate_id();
        match s3
            .head_object(HeadObjectRequest {
                bucket: S3_BUCKET.clone(),
                key: id.clone(),
                ..Default::default()
            })
            .await
        {
            Ok(_) => {}
            Err(RusotoError::Unknown(BufferedHttpResponse {
                status: StatusCode::NOT_FOUND,
                ..
            })) => return Ok(id),
            Err(e) => return Err(anyhow::Error::new(e)),
        }
    }
}

fn serialize_statuscode<S>(status_code: &StatusCode, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_u16(status_code.as_u16())
}

#[derive(Debug, serde::Serialize)]
pub struct AppError {
    #[serde(serialize_with = "serialize_statuscode")]
    pub status: StatusCode,
    pub message: String,
}

impl IntoResponse for AppError {
    type Body = Full<Bytes>;

    type BodyError = Infallible;

    fn into_response(self) -> axum::http::Response<Self::Body> {
        (self.status, Json(json!(&self))).into_response()
    }
}

impl From<anyhow::Error> for AppError {
    fn from(err: anyhow::Error) -> Self {
        let mut status = StatusCode::INTERNAL_SERVER_ERROR;
        let mut message = "Internal server error";

        let cause = err.root_cause();

        if let Some(GetObjectError::NoSuchKey(_)) = cause.downcast_ref::<GetObjectError>() {
            status = StatusCode::NOT_FOUND;
            message = "Not found";
        } else {
            tracing::error!("internal error: {:?}", &cause);
        }

        AppError {
            status,
            message: message.to_string(),
        }
    }
}

fn anyhow_reject<E: std::error::Error + Sync + Send + 'static>(err: E) -> anyhow::Error {
    anyhow::Error::new(err)
}

async fn retrieve(
    Path(id): Path<String>,
    Extension(s3): Extension<S3Client>,
) -> Result<impl IntoResponse, AppError> {
    let obj = s3
        .get_object(GetObjectRequest {
            key: id,
            bucket: S3_BUCKET.clone(),
            ..Default::default()
        })
        .await
        .map_err(anyhow_reject)?;

    let mut headers = vec![];
    if let Some(content_disposition) = obj.content_disposition {
        headers.push((axum::http::header::CONTENT_DISPOSITION, content_disposition));
    }

    let body = obj
        .body
        .map(|b| StreamBody::new(b).into_response())
        .unwrap();

    Ok((Headers(headers), body))
}

async fn upload(
    ContentLengthLimit(mut payload): ContentLengthLimit<Multipart, { 50 * 1000 * 1000 }>,
    Extension(s3): Extension<S3Client>,
) -> Result<impl IntoResponse, AppError> {
    let mut ids = vec![];

    while let Some(field) = payload.next_field().await.map_err(anyhow_reject)? {
        let id = generate_unique_id(&s3).await?;
        match field.name() {
            Some("file") => {}
            Some(field_name) => {
                tracing::debug!("ignored field: {}", field_name);
                continue;
            }
            _ => continue,
        }

        let _ = s3
            .put_object(PutObjectRequest {
                key: id.clone(),
                content_type: field.content_type().map(ToString::to_string),
                content_disposition: Some(format!(
                    "inline; filename={}",
                    field
                        .file_name()
                        .map(ToOwned::to_owned)
                        .unwrap_or_else(|| id.clone())
                )),
                body: Some(field.bytes().await.unwrap().to_vec().into()),
                bucket: S3_BUCKET.clone(),
                ..Default::default()
            })
            .await
            .map_err(anyhow_reject)?;

        ids.push(id);
    }

    Ok(Json(json!(ids
        .into_iter()
        .map(|id| format!("{}/{}", *URL, id))
        .collect::<Vec<_>>())))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().expect("Failed to read .env file");
    lazy_static::initialize(&ADDRESS);
    lazy_static::initialize(&URL);
    lazy_static::initialize(&S3_BUCKET);
    lazy_static::initialize(&S3_REGION);

    tracing_subscriber::fmt::init();
    EnvironmentProvider::default().credentials().await?;

    // let s3 = S3Client::new(rusoto_core::Region::Custom {
    //     name: S3_REGION.clone().to_owned(),
    //     endpoint: "s3.net".to_owned(),
    // });
    let s3 = S3Client::new(Region::from_str(&S3_REGION).unwrap());
    let app = Router::new()
        .route("/:id", get(retrieve))
        .route("/", post(upload))
        .layer(AddExtensionLayer::new(s3));

    axum::Server::bind(&ADDRESS)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}