PacketRecorder
=====================

Records and replays packet traffic. Designed to flexibly capture packet traffic in an enterprise-scale environment. Captured data can be analyzed or recreated exactly to simulate how it would have appeared on the frontend.

I posted this code as an example of my work. While the code is functional, this is a small subset of a fully implemented proprietary deployment. The code here is the core of the project's backend and database-interface layer. It represents the most general-purpose and domain-unspecific portions of the project.

**Features on display in this sample:**
- A cassandra configuration providing efficient storage and random access for a many-terabyte-scale data set.
- Efficient deduplication engine to drastically reduce on-disk storage size, depending on the nature of the data set.
- Data streaming for recording and replaying traffic.
- Linearly scalable to thousands of concurrent recordings and replays per second.
- Negligible performance impact.

<br>

## Technology Stack:

**Java:** All of the code here is written in java. 

**<a href="https://maven.apache.org/">Maven:**</a> Build configuration done with Apache Maven.

**<a href="https://diwakergupta.github.io/thrift-missing-guide/">Thrift:</a>** Apache Thrift is used to serialize captured data in a semi-structured way as well as generating java objects for the recorded data. This makes it easier to maintain and interact with a large dataset that is very likely to change over time. Thrift objects are serialized using thrift's "compact binary protocol", the most efficient thrift-serialization protocol in common use.

**<a href="http://cassandra.apache.org/">Cassandra:</a>** The Apache Cassandra distributed DBMS is used to store serialized thrift objects and to capture big-picture relationships between data objects. It scales linearly to huge levels with commodity hardware and it provides high availability, replication, data expiration, on-disk compression, and random access to a huge data set of recordings. PacketRecorder uses specifically the <a href="http://docs.datastax.com/en/drivers/java/2.0/">datastax java driver</a> and <a href="https://docs.datastax.com/en/cql/3.1/cql/cql_intro_c.html">CQL query language</a> to interface with Cassandra. CQL has many similarities to SQL but should not be used without a knowledge of cassandra internals.

**<a href="https://redis.io/">Redis:</a>** The Redis in-memory key-value store is used optionally as a cache for deduplication in order to provide efficient, network-wide minimum recurrence thresholds for deduplication. 

<br>

## Deduplication:

Deduplicating captured data across recording streams can massively reduce disk usage, depending on the use case. In my primary use case, deduplication reduced disk usage by 100x compared to raw packet traffic. 

PacketRecorder allows a broadly distributed network to detect duplicate data between its many recording streams and store the data once rather than multiple times. Duplicates are detected by hashing blocks of recorded data with a sufficiently astronomically low chance of hash collision. In order to provide efficient and dynamic duplicate detection, the hashes are then stored with a counter in redis. If the counter surpasses a configured limit, then a single deduplicated copy of the hashed data is stored in Cassandra and any further instances of that data are referred to by its hash rather than its full contents. When data is subsequently streamed from the database, the recording chunks are first streamed in and then any deduplicated chunks are fetched as they are needed.

<br>

## RAM Footprint:

Recording lots of packet traffic means recording huge quantities of data. It stands to reason that memory usage might be a problem. However, data is streamed to and from the database in chunks as it is recorded and replayed so that memory can be freed up as soon as possible. As a result, in most use cases memory usage will be mostly or entirely negligible. 

<br>

## CPU Footprint:

PacketRecorder does little computation, so its CPU footprint is minimal. Hashing for deduplication is the only nontrivial workload, it is optional, and it can be done totally asynchronous to normal packet processing. 


