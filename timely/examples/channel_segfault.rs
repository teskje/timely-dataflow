use timely::dataflow::{InputHandle, Scope};
use timely::dataflow::operators::{Input, Exchange};

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let rounds = std::env::args().nth(1).unwrap().parse::<usize>().unwrap();

        loop {
            let mut input = InputHandle::new();

            let dataflow_id = worker.dataflow(|scope| {
                scope
                    .input_from(&mut input)
                    .exchange(|&x| x as u64);
                scope.addr()[0]
            });

            for round in 0..rounds {
                input.send(round);
                input.advance_to(round);
                worker.step();
            }

            drop(input);
            worker.drop_dataflow(dataflow_id);

            if dataflow_id % 100 == 0 {
                println!("worker {}: {}", worker.index(), dataflow_id);
            }
        }
    }).unwrap();
}
