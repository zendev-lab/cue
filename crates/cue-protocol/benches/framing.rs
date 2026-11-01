use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use cue_protocol::{ClientId, Hello, Message, Query, RequestId, decode_message, encode_message};

fn framing(criterion: &mut Criterion) {
    let message = Message::Query {
        request_id: RequestId::new(1).unwrap(),
        query: Query::Hello(Hello {
            protocol_version: cue_protocol::PROTOCOL_VERSION,
            client_id: ClientId::new("benchmark-client").unwrap(),
        }),
    };
    let frame = encode_message(&message).unwrap();
    assert_eq!(decode_message(&frame).unwrap(), message);
    criterion.bench_function("IPC v5 encode Hello", |bencher| {
        bencher.iter(|| encode_message(black_box(&message)).unwrap());
    });
    criterion.bench_function("IPC v5 decode Hello", |bencher| {
        bencher.iter(|| decode_message(black_box(&frame)).unwrap());
    });
}

criterion_group!(benches, framing);
criterion_main!(benches);
