CREATE TABLE history.test_indexes (
    id text PRIMARY KEY,
    data blob
) WITH default_time_to_live = 0;

CREATE TABLE history.ranges (
    id text PRIMARY KEY,
    data blob
) WITH default_time_to_live = 0;