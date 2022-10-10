# Production Readiness

In general, the latest [release](https://github.com/tikv/client-java/releases) of TiKV Java Client is ready for production use. But it is not battle-tested as full featured client for TiKV in all use cases. This page will give you more details.

## RawKV
All RawKV APIs are covered by [CI](https://github.com/tikv/client-java/actions/workflows/ci.yml).

At this time, RawKV has been used in the production environment of some commercial customers in latency sensitive systems. But they only use part of the RawKV APIs (mainly including `raw_put`, `raw_get`, `raw_compare_and_swap`, and `raw_batch_put`).

## TxnKV
All TxnKV APIs are covered by [CI](https://github.com/tikv/client-java/actions/workflows/ci.yml).

In addition, TxnKV has been used in the [TiSpark](https://docs.pingcap.com/tidb/stable/tispark-overview) and [TiBigData](https://github.com/tidb-incubator/TiBigData) project to integrate data from TiDB to Big Data ecosystem. TiSpark and TiBigData were used in the production system of some commercial customers and some internet companies.

Similar to RawKV, only part of APIs are used in this scenario (mainly including `prewrite/commit` and `coprocessor`). And this use case doesn't care about latency but throughput and reliability.

## TiDB Cloud
Directly using TiKV is not possible on TiDB Cloud due to the fact that client has to access the whole cluster, which has security issues. And TiKV managed service is not coming soon as it's not contained in [roadmap](https://docs.pingcap.com/tidbcloud/tidb-cloud-roadmap) yet.
