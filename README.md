# riak-bucket-export
Exports RIAK bucket to JSON file

## Installation
Just place script anywhere and run

## Usage
Example:

```bash
./riak-bucket-dump.py -C 1000 -b mytype mybucket
```

Connects to localhost:8098 and dumps bucket `mybucket` with type `mytype` using 20 workers. Every worker is restared after 1000 batches in order to prevent memory leaks. Output file is written to `mybucket.json`. See `./riak-bucket-dump.py -h` for more options.
