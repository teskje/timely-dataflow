use std::time::Duration;

use timely::dataflow::operators::{Input, Inspect};
use timely::dataflow::InputHandle;
use timely::logging::{Logger, WorkerIdentifier};
use timely::worker::AsWorker;

extern crate timely;

const LOGGER_NAME: &str = "my-custom-logger";

type Event = String;

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        // register a custom logger
        worker.log_register().insert(
            LOGGER_NAME,
            |_time, batch: &mut Vec<(Duration, WorkerIdentifier, Event)>| {
                for event in batch {
                    println!("event: {event:?}");
                }
            },
        );

        // get a named logger from the worker
        let logger: Logger<Event> = worker.log_register().get(LOGGER_NAME).unwrap();
        logger.log("initializing");

        let mut input = InputHandle::new();
        worker.dataflow(|scope| {
            // get a named logger from a scope
            let logger: Logger<Event> = scope.log_register().get(LOGGER_NAME).unwrap();

            scope
                .input_from(&mut input)
                .inspect(move |i| logger.log(format!("saw data: {i}")));
        });

        logger.log("processing");
        for i in 0..5 {
            input.send(i);
            input.advance_to(i);
            worker.step();
        }
        logger.log("done");
    });
}
