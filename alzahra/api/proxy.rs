use crate::ActiveShips;
use crate::models::Horp;
use actix_web::{
    HttpResponse, Scope, post,
    web::{self, Data},
};
use shared::{shipment::SpringTank, utils::Buffer};
use shared::{
    shipment::{BinDencode, Shipment},
    uid::UniqueId,
};
use std::sync::atomic::Ordering;
use std::{collections::HashMap, io::Cursor};

#[post("/bin-batch/")]
async fn r_bin_batch(payload: web::Bytes, ships: Data<ActiveShips>) -> Horp {
    let mut reader = Cursor::new(payload);
    let shipment = Shipment::read(&mut reader).await?;

    let ship = ships.ship_get(shipment.ship_uid).await;
    let mut ship = ship.lock().await;

    if shipment.order <= ship.latest_order {
        return crate::err!(BadRequest, "shipment order has already consmumed");
    }

    if shipment.order > ship.latest_order + 1 {
        ship.queued_shipments.push(shipment);
        log::warn!("return the acumelated data");
        // TODO: return the acumelated buffer
        return Ok(HttpResponse::Ok().finish());
    }

    ship.latest_order = shipment.order;

    for ch in shipment.tanks {
        let Some(schr) = ship.springs.get(&ch.id) else {
            if ch.ended && ch.data.is_empty() {
                continue;
            }
            ship.new_channel(&ch).await;
            continue;
        };

        if schr.ended.load(Ordering::Relaxed) {
            continue;
        }

        for buf in Buffer::from_data(&ch.data) {
            let _ = schr.sx.send(buf).await;
        }
        if ch.ended {
            schr.ended.store(true, Ordering::Relaxed);
        }
    }

    let data_collection = std::time::Instant::now();
    let mut data_collected_len = 0;
    let mut tanks = HashMap::<UniqueId, SpringTank>::new();
    loop {
        if ship.springs.is_empty() {
            break;
        }

        ship.springs.retain(|_, s| {
            if data_collected_len >= 30 * 1024 * 1024 {
                return true;
            }

            if let Some(tank) = tanks.get_mut(&s.id) {
                let before = tank.data.len();
                let ended = s.ended.load(Ordering::Relaxed)
                    || s.read_data(&mut tank.data);
                data_collected_len += tank.data.len().saturating_sub(before);
                tank.ended = ended;
                return !ended;
            }

            let tank = s.to_tank();
            let ended = tank.ended;
            if ended || !tank.data.is_empty() {
                tanks.insert(tank.id, tank);
            }
            !ended
        });

        if data_collection.elapsed().as_millis() >= 500
            || data_collected_len >= 30 * 1024 * 1024
        {
            break;
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    ship.response_order += 1;
    let response_shipment = Shipment {
        ship_uid: shipment.ship_uid,
        tanks: tanks.into_values().collect(),
        order: ship.response_order,
    };

    let body = response_shipment.to_bytes().await;
    Ok(HttpResponse::Ok().body(body))
}

pub fn router() -> Scope {
    Scope::new("/proxy").service(r_bin_batch)
}
