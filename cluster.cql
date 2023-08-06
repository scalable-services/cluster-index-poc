CREATE KEYSPACE history WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE history.temporal_indexes (
    id text PRIMARY KEY,
    buf blob
) WITH default_time_to_live = 0;

CREATE TABLE history.indexes (
    id text PRIMARY KEY,
    buf blob
) WITH default_time_to_live = 0;

CREATE TABLE history.blocks (
    partition text,
    id text,
    bin blob,
    size int,
    PRIMARY KEY(partition, id)
) WITH default_time_to_live = 0;

CREATE TABLE history.test_indexes (
    id text PRIMARY KEY,
    data blob
) WITH default_time_to_live = 0;

CREATE TABLE history.ranges (
    id text PRIMARY KEY,
    data blob
) WITH default_time_to_live = 0;

