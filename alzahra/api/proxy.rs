use crate::models::Horp;
use crate::{ActiveShips, config::Config};
use actix_web::{HttpResponse, Scope, post, web::Data};
use base64::Engine;
use shared::shipment::SpringTank;
use shared::{
    shipment::{BinDencode, Shipment},
    uid::UniqueId,
};
// use std::sync::atomic::Ordering;
use std::{collections::HashMap, io::Cursor};

#[post("/bin-batch/")]
async fn r_bin_batch(body: String, ships: Data<ActiveShips>) -> Horp {
    let conf = Config::get();
    let Ok(data) = conf.b64.decode(body) else {
        return crate::err!(BadRequest, "invalid base64");
    };

    let mut reader = Cursor::new(data);
    let Ok(shipment) = Shipment::read(&mut reader).await else {
        return crate::err!(BadRequest, "invalid shipment");
    };

    let ship_id = shipment.ship_id;
    let ship = ships.ship_get(ship_id).await;
    let mut ship = ship.lock().await;

    if shipment.order <= ship.latest_order {
        // do nothing...
    } else if shipment.order > ship.latest_order + 1 {
        ship.queued_shipments.push(shipment.clone());
    } else {
        ship.spring_update(&shipment).await;
    }

    ship.queued_shipments.sort_by_key(|o| o.order);
    let queued = ship.queued_shipments.clone();
    let mut queue_prog = 0;
    for q in queued.iter() {
        if ship.latest_order + 1 != q.order {
            break;
        }
        ship.spring_update(q).await;
        queue_prog += 1;
    }
    ship.queued_shipments.drain(0..queue_prog);

    if shipment.order_request != 0
        && shipment.order_request <= ship.response_order
    {
        let Some(bo) = ship
            .order_backlog
            .iter()
            .find(|o| o.order == shipment.order_request)
        else {
            ship.reset();
            let s = Shipment {
                ship_id,
                order: 0,
                reset: true,
                order_request: 0,
                tanks: Vec::new(),
            };
            let body = conf.b64.encode(s.to_bytes().await);
            return Ok(HttpResponse::Ok().body(body));
        };

        let body = conf.b64.encode(bo.to_bytes().await);
        return Ok(HttpResponse::Ok().body(body));
    }

    // let data_collection = std::time::Instant::now();
    let mut data_collected_len = 0;
    let mut tanks = HashMap::<UniqueId, SpringTank>::new();
    let mut removal = Vec::with_capacity(ship.springs.len());
    for (sid, s) in ship.springs.iter_mut() {
        if data_collected_len >= 7 * 1024 * 1024 {
            break;
        }

        let tank = s.to_tank().await;
        let ended = tank.ended;
        data_collected_len += tank.data.len();
        if ended || !tank.data.is_empty() {
            tanks.insert(tank.id, tank);
        }
        if ended {
            removal.push(*sid);
        }
    }
    for sid in removal.iter() {
        ship.springs.remove(sid);
    }

    // loop {
    //     if ship.springs.is_empty() {
    //         break;
    //     }
    //
    //     let mut removal = Vec::with_capacity(ship.springs.len());
    //     for (sid, s) in ship.springs.iter_mut() {
    //         if data_collected_len >= 3 * 1024 * 1024 {
    //             break;
    //         }
    //
    //         if let Some(tank) = tanks.get_mut(&s.id) {
    //             let before = tank.data.len();
    //             let ended = s.ended.load(Ordering::Relaxed)
    //                 || s.read_data(&mut tank.data).await;
    //             data_collected_len += tank.data.len().saturating_sub(before);
    //             tank.ended = ended;
    //             if ended {
    //                 removal.push(*sid);
    //             }
    //             continue;
    //         }
    //
    //         let tank = s.to_tank().await;
    //         let ended = tank.ended;
    //         if ended || !tank.data.is_empty() {
    //             tanks.insert(tank.id, tank);
    //         }
    //         if ended {
    //             removal.push(*sid);
    //         }
    //     }
    //     for sid in removal.iter() {
    //         ship.springs.remove(sid);
    //     }
    //
    //     if data_collection.elapsed().as_millis() >= 200
    //         || data_collected_len >= 3 * 1024 * 1024
    //     {
    //         break;
    //     }
    //
    //     tokio::time::sleep(std::time::Duration::from_millis(70)).await;
    // }

    ship.response_order += 1;
    let response_shipment = Shipment {
        ship_id,
        order_request: 0,
        reset: false,
        tanks: tanks.into_values().collect(),
        order: ship.response_order,
    };

    ship.new_backlog(response_shipment.clone());
    let body = response_shipment.to_bytes().await;
    let body = conf.b64.encode(body);
    Ok(HttpResponse::Ok().body(body))
}

pub fn router() -> Scope {
    Scope::new("/proxy").service(r_bin_batch)
}
