use std::time::Duration;

use timely::dataflow::operators::{Exchange, Input, Inspect};
use timely::dataflow::InputHandle;
use timely::logging::{TimelyEvent, WorkerIdentifier};

extern crate timely;

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        // register a logger for Timely events
        worker.log_register().insert(
            "timely",
            |_time, batch: &mut Vec<(Duration, WorkerIdentifier, TimelyEvent)>| {
                for event in batch {
                    println!("event: {event:?}");
                }
            },
        );

        let mut input = InputHandle::new();
        worker.dataflow(|scope| {
            scope.input_from(&mut input).exchange(|i| *i);
        });

        for i in 0..5 {
            input.send(i);
            input.advance_to(i);
            worker.step();
        }
    });
}
