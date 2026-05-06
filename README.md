# task_dispatcher

A concurrent task dispatcher written in Rust. A seeded generator produces tasks at a steady rate, a dispatcher buffers and routes them through a FIFO queue, and a pool of eight worker threads executes them in parallel. Two experiments run back-to-back and each writes its results to disk so you can review them after the program exits.

## Running it

```bash
cd FInal_project
cargo build
cargo run
```

Just want to check it compiles without running the simulation?

```bash
cd FInal_project
cargo check
```

Results are saved to `results/balance.txt` and `results/cpu.txt` after each run.

## What it does

Tasks arrive from a seeded generator at a steady rate. Each task carries an id, an arrival timestamp, a type (CPU or IO), and a fixed duration. The dispatcher holds a `VecDeque` buffer and forwards tasks to workers in arrival order. Eight worker threads share a single channel receiver behind a mutex and pull tasks as they become available.

CPU tasks busy-spin for their duration to simulate compute-bound work. IO tasks call sleep to simulate waiting on an external resource. That difference in behavior is what makes the two experiments meaningful to compare.

## The two experiments

- **Balanced** (50/50, seed 42) — half the tasks sleep and half spin. Because IO tasks yield the CPU, workers stay unblocked more often, the queue drains faster, and average wait times stay lower.
- **CPU-heavy** (80/20, seed 99) — the majority of tasks spin the CPU. Workers stay occupied longer, the queue builds up more quickly, and both wait times and makespan increase noticeably.

Both seeds are fixed so the task mix is identical every run. Exact timing numbers will vary by machine, but the relative difference between the two experiments is reproducible.

## Metrics

Each experiment reports the following after all tasks complete:

- total tasks completed
- makespan
- average and max wait time
- average turnaround time
- peak queue length
- worker utilization

Utilization is calculated as total busy time across all workers divided by makespan times worker count.

## How shutdown works

The program does not use sleep-based guessing to decide when to stop. The generator closes its channel once it finishes sending all tasks. The dispatcher detects the disconnection, drains any tasks still sitting in the buffer, sends a `Shutdown` message to each worker, then exits. Workers break out of their loop on receiving `Shutdown` or on a channel error. All thread handles are joined in order before results are written to disk.

## Tool use

GitHub Copilot was used during development, mainly for help with channel wiring and structuring the dispatcher loop. Not everything it suggested worked correctly out of the box — the queue length tracking was recording at the wrong point in the loop and had to be moved to every `push_back` call so the peak queue length reflects the actual high-water mark of the buffer.
