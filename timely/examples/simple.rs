use timely::dataflow::operators::*;

#[tokio::main(flavor = "local")]
async fn main() {
    timely::example(|scope| {
        (0..10).to_stream(scope)
               .inspect(|x| println!("seen: {:?}", x));
    }).await;
}

