use std::io::Cursor;

use crate::err;
use crate::models::Horp;
use actix_web::{
    HttpResponse, Scope, post,
    web::{self, Buf},
};
use futures_util::StreamExt;
use shared::shipment::{BinDencode, Shipment};

#[post("/bin-batch/")]
async fn r_bin_batch(payload: web::Bytes) -> Horp {
    let mut reader = Cursor::new(payload);
    let shipment = Shipment::read(&mut reader).await?;

    log::info!("shipment: {shipment:#?}");

    // while let Some(chunk) = payload.next().await {
    //     let Ok(chunk) = chunk else {
    //         return err!(BadRequest, "error reading payload");
    //     };
    //
    //
    // }

    Ok(HttpResponse::Ok().finish())
}

pub fn router() -> Scope {
    Scope::new("/proxy").service(r_bin_batch)
}
