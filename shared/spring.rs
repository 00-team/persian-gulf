use crate::{shipment::SpringTank, uid::UniqueId, utils::SocksHost};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::mpsc::{self, error::TryRecvError};

#[derive(Debug)]
pub struct Spring {
    pub id: UniqueId,
    pub host: SocksHost,
    pub port: u16,
    pub sx: mpsc::Sender<Vec<u8>>,
    pub rx: mpsc::Receiver<Vec<u8>>,
    pub ended: Arc<AtomicBool>,
}

impl Spring {
    pub fn read_data(&mut self, data: &mut Vec<u8>) -> bool {
        loop {
            match self.rx.try_recv() {
                Ok(v) => {
                    data.extend_from_slice(&v);
                }
                Err(e) => match e {
                    TryRecvError::Empty => break,
                    TryRecvError::Disconnected => return true,
                },
            }
        }

        false
    }

    pub fn to_tank(&mut self) -> SpringTank {
        let mut data = Vec::with_capacity(100 * 1024);
        let mut ended = self.ended.load(Ordering::Relaxed);

        ended = ended || self.read_data(&mut data);

        SpringTank {
            id: self.id,
            port: self.port,
            host: self.host.clone(),
            ended,
            data,
        }
    }
}
