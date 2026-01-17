use std::time::{Instant, Duration};

use timely::Config;
use timely::synchronization::Sequencer;

#[tokio::main(flavor = "local")]
async fn main() {
    timely::execute(Config::process(4), async |worker| {

        let timer = Instant::now();
        let mut sequencer = Sequencer::new(worker, Instant::now());

        for round in 0 .. {
            // if worker.index() < 3 {
                tokio::time::sleep(Duration::from_secs(1 + worker.index() as u64)).await;
                sequencer.push(format!("worker {:?}, round {:?}", worker.index(), round));
            // }
            for element in &mut sequencer {
                println!("{:?}:\tWorker {:?}:\t recv'd: {:?}", timer.elapsed(), worker.index(), element);
            }
            worker.step().await;
        }

    }).await.unwrap().join_and_assert().await;
}
