## Development

you can run this by running one coordinator:

```
cargo run coordinator -n 10
```
where the `-n` option specifies the number of buckets/reduce tasks you want to run

and also running one or more workers at the same time:
```
cargo run worker -i 1 &;cargo run worker -i 2 &;
```
where the `-i` option specifies the id of the worker for when it prints out what task it's working on

This is basically a rough implementation of an MIT MapReduce lab assignment in Rust.

It's Map Reduce running but instead of having different machines, the "nodes" are just separate proceses.
There is one "coordinator" process and many "worker" processes.

The coordinator keeps the state of what tasks need to get done and the workers
are responsible for asking for a task from the coordinator and then doing it.

The coordinator checks 10 seconds after a task is assigned to make sure it got done,
or else it will put it back in the todo list for it be reasssigned. The coordinator
starts a new thread for each incoming request it gets from a worker so we have to
put the state of the Coordinator behind a `Arc<Mutex>` to make sure there aren't
any concurrency issues.

The coordinator and workers communicate through TCP.

The map and reduce functions defined in `worker.rs` implement a distributed word count
(like the MIT lab specifies), and the files that should be used for the word count
should be placed in the `files` directory in the root of the project.

NOTE: the files must have the `.txt` file extension for the coordinator to detect them.

To run the distributed word count, put the files you want to run the count on in a
directory named "files" in the repository.

I included a python script for verification of results.
