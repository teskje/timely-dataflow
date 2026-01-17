use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Input;
use timely::dataflow::InputHandle;
use timely::Config;

#[tokio::test(flavor = "local")] async fn operator_scaling_1() { operator_scaling(1).await; }
#[tokio::test(flavor = "local")] async fn operator_scaling_10() { operator_scaling(10).await; }
#[tokio::test(flavor = "local")] async fn operator_scaling_100() { operator_scaling(100).await; }
#[tokio::test(flavor = "local")] #[cfg_attr(miri, ignore)] async fn operator_scaling_1000() { operator_scaling(1000).await; }
#[tokio::test(flavor = "local")] #[cfg_attr(miri, ignore)] async fn operator_scaling_10000() { operator_scaling(10000).await; }
#[tokio::test(flavor = "local")] #[cfg_attr(miri, ignore)] async fn operator_scaling_100000() { operator_scaling(100000).await; }

async fn operator_scaling(scale: u64) {
    timely::execute(Config::thread(), async move |worker| {
        let mut input = InputHandle::new();
        worker.dataflow::<u64, _, _>(|scope| {
            use timely::dataflow::operators::Partition;
            let parts =
            scope
                .input_from(&mut input)
                .partition(scale, |()| (0, ()));

            use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
            let mut builder = OperatorBuilder::new("OpScaling".to_owned(), scope.clone());
            let mut handles = Vec::with_capacity(parts.len());
            let mut outputs = Vec::with_capacity(parts.len());
            for (index, part) in parts.into_iter().enumerate() {
                let (output, stream) = builder.new_output_connection::<Vec<()>,_>([]);
                use timely::progress::Antichain;
                let connectivity = [(index, Antichain::from_elem(Default::default()))];
                handles.push((builder.new_input_connection(&part, Pipeline, connectivity), output));
                outputs.push(stream);
            }

            builder.build(move |_| {
                move |_frontiers| {
                    for (input, output) in handles.iter_mut() {
                        let mut output = output.activate();
                        input.for_each(|time, data| {
                            output.give(&time, data);
                        });
                    }
                }
            });
        });
    })
    .await
    .unwrap()
    .join_and_assert()
    .await;
}

#[tokio::test(flavor = "local")] async fn subgraph_scaling_1() { subgraph_scaling(1).await; }
#[tokio::test(flavor = "local")] async fn subgraph_scaling_10() { subgraph_scaling(10).await; }
#[tokio::test(flavor = "local")] async fn subgraph_scaling_100() { subgraph_scaling(100).await; }
#[tokio::test(flavor = "local")] #[cfg_attr(miri, ignore)] async fn subgraph_scaling_1000() { subgraph_scaling(1000).await; }
#[tokio::test(flavor = "local")] #[cfg_attr(miri, ignore)] async fn subgraph_scaling_10000() { subgraph_scaling(10000).await; }
#[tokio::test(flavor = "local")] #[cfg_attr(miri, ignore)] async fn subgraph_scaling_100000() { subgraph_scaling(100000).await; }

async fn subgraph_scaling(scale: u64) {
    timely::execute(Config::thread(), async move |worker| {
        let mut input = InputHandle::new();
        worker.dataflow::<u64, _, _>(|scope| {
            use timely::dataflow::operators::Partition;
            let parts =
            scope
                .input_from(&mut input)
                .partition(scale, |()| (0, ()));

            use timely::dataflow::Scope;
            let _outputs = scope.region(|inner| {
                use timely::dataflow::operators::{Enter, Leave};
                parts.into_iter().map(|part| part.enter(inner).leave()).collect::<Vec<_>>()
            });
        });
    })
    .await
    .unwrap()
    .join_and_assert()
    .await;
}
