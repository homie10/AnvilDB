use db_core::Command;
use db_raft::RaftCluster;
use server::{ApiService, ClientRequest, NodeAddress, ReadConsistency, RequestEnvelope, Transport};

fn main() {
    let cluster = RaftCluster::new(vec![1, 2, 3]).expect("cluster bootstrap must succeed");
    let mut service = ApiService::new(cluster);

    let client = NodeAddress::new(10_001, "client://demo");
    let leader = service.leader_address();

    let mut transport = service.in_process_transport();

    let write_response = transport
        .round_trip(RequestEnvelope {
            request_id: 1,
            from: client.clone(),
            to: leader.clone(),
            timeout_ticks: None,
            request: ClientRequest::Write(Command::Put {
                key: "planet".to_owned(),
                value: "saturn".to_owned(),
            }),
        })
        .expect("in-process transport should return a write response");
    println!("{write_response:#?}");

    let read_response = transport
        .round_trip(RequestEnvelope {
            request_id: 2,
            from: client.clone(),
            to: leader.clone(),
            timeout_ticks: None,
            request: ClientRequest::Read {
                key: "planet".to_owned(),
                consistency: ReadConsistency::Linearizable,
            },
        })
        .expect("in-process transport should return a read response");
    println!("{read_response:#?}");

    let sql_response = transport
        .round_trip(RequestEnvelope {
            request_id: 3,
            from: client,
            to: leader,
            timeout_ticks: None,
            request: ClientRequest::Sql {
                query: "SELECT value FROM kv WHERE key = 'planet'".to_owned(),
                read_consistency: ReadConsistency::Linearizable,
            },
        })
        .expect("in-process transport should return a SQL response");
    println!("{sql_response:#?}");
}
