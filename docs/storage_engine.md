# Storage Engine

## Feature Requirements

Currently, we have the following feature requirements for the storage engine:
- Support snapshot read;
- Support bidirectional traversal;
- Supports read-write transactions or write transactions with CAS function;
- Expose logic clock.

The following levels of transaction assurance are required for the storage engine:
- Isolation Guarantee: Snapshot Isolation;
- Session Guarantee: Linearizable.

Possible anomalies to Snapshot Isolation as summarised in [HATs]((http://www.vldb.org/pvldb/vol7/p181-bailis.pdf)):

- Write Skew: The current data structure and write design theoretically do not break the consistency constraint;
- Phantom: Using Snapshot Read, theoretically, reading the value written by concurrent transactions in advance does not affect the final consistency of the data.

## The Current Definition of the Storage Engine Interface

The storage engine is abstracted as an [interface](../pkg/storage/interface.go), and may be further optimized in the future. The goal is to make it possible to adapt to more KV databases.
