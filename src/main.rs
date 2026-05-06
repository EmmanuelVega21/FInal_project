use rand::{rngs::StdRng, Rng, SeedableRng};
use std::collections::VecDeque;
use std::fmt::Write as FmtWrite;
use std::fs::{create_dir_all, File};
use std::io::{self, Write};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub enum TaskType {
    CPU,
    IO,
}

#[derive(Clone, Debug)]
pub struct Task {
    pub id: usize,
    pub arrival_time: Instant,
    pub kind: TaskType,
    pub duration: Duration,
}

#[derive(Clone, Debug)]
pub enum WorkerMessage {
    Task(Task),
    Shutdown,
}

pub struct Metrics {
    pub total_completed: usize,
    pub total_wait_time: Duration,
    pub total_turnaround: Duration,
    pub total_busy_time: Duration,
    pub max_wait_time: Duration,
    pub max_queue_length: usize,
    pub worker_count: usize,
    pub start_time: Instant,
}

impl Metrics {
    pub fn new(worker_count: usize) -> Self {
        Self {
            total_completed: 0,
            total_wait_time: Duration::ZERO,
            total_turnaround: Duration::ZERO,
            total_busy_time: Duration::ZERO,
            max_wait_time: Duration::ZERO,
            max_queue_length: 0,
            worker_count,
            start_time: Instant::now(),
        }
    }

    pub fn record(&mut self, task: Task, start: Instant, finish: Instant) {
        self.total_completed += 1;
        let wait = start.duration_since(task.arrival_time);
        let turnaround = finish.duration_since(task.arrival_time);
        let busy_time = finish.duration_since(start);

        self.total_wait_time += wait;
        self.total_turnaround += turnaround;
        self.total_busy_time += busy_time;

        if wait > self.max_wait_time {
            self.max_wait_time = wait;
        }
    }

    pub fn observe_queue_length(&mut self, queue_length: usize) {
        if queue_length > self.max_queue_length {
            self.max_queue_length = queue_length;
        }
    }

    pub fn average_wait_time(&self) -> Duration {
        if self.total_completed == 0 {
            Duration::ZERO
        } else {
            self.total_wait_time / self.total_completed as u32
        }
    }

    pub fn average_turnaround_time(&self) -> Duration {
        if self.total_completed == 0 {
            Duration::ZERO
        } else {
            self.total_turnaround / self.total_completed as u32
        }
    }

    pub fn worker_utilization(&self) -> f64 {
        let makespan = self.makespan().as_secs_f64();
        if makespan == 0.0 || self.worker_count == 0 {
            0.0
        } else {
            self.total_busy_time.as_secs_f64() / (makespan * self.worker_count as f64)
        }
    }

    pub fn makespan(&self) -> Duration {
        self.start_time.elapsed()
    }

    pub fn render(&self) -> String {
        let mut report = String::new();
        let _ = writeln!(&mut report, "Total completed: {}", self.total_completed);
        let _ = writeln!(&mut report, "Avg wait: {:?}", self.average_wait_time());
        let _ = writeln!(&mut report, "Max wait: {:?}", self.max_wait_time);
        let _ = writeln!(&mut report, "Avg turnaround: {:?}", self.average_turnaround_time());
        let _ = writeln!(&mut report, "Peak queue length: {}", self.max_queue_length);
        let _ = writeln!(&mut report, "Worker utilization: {:.2}%", self.worker_utilization() * 100.0);
        let _ = writeln!(&mut report, "Makespan: {:?}", self.makespan());
        report
    }
}

pub fn start_generator(
    tx: mpsc::Sender<Task>,
    total: usize,
    cpu_probability: f64,
    seed: u64,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut rng = StdRng::seed_from_u64(seed);

        for id in 0..total {
            let random_value = rng.next_u64() as f64 / u64::MAX as f64;
            let kind = if random_value < cpu_probability {
                TaskType::CPU
            } else {
                TaskType::IO
            };

            let task = Task {
                id,
                arrival_time: Instant::now(),
                kind,
                duration: Duration::from_millis(200),
            };

            tx.send(task).expect("failed to send task");
            thread::sleep(Duration::from_millis(20));
        }
    })
}

pub fn start_dispatcher(
    rx: mpsc::Receiver<Task>,
    worker_tx: mpsc::Sender<WorkerMessage>,
    worker_count: usize,
    metrics: Arc<Mutex<Metrics>>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut buffer: VecDeque<Task> = VecDeque::new();
        let mut closed = false;

        loop {
            while let Ok(task) = rx.try_recv() {
                buffer.push_back(task);
                metrics.lock().unwrap().observe_queue_length(buffer.len());
            }

            if let Some(task) = buffer.pop_front() {
                if worker_tx.send(WorkerMessage::Task(task)).is_err() {
                    return;
                }
                continue;
            }

            if closed {
                break;
            }

            match rx.recv_timeout(Duration::from_millis(5)) {
                Ok(task) => {
                    buffer.push_back(task);
                    metrics.lock().unwrap().observe_queue_length(buffer.len());
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => closed = true,
            }
        }

        for _ in 0..worker_count {
            let _ = worker_tx.send(WorkerMessage::Shutdown);
        }
    })
}

pub fn start_workers(
    worker_count: usize,
    rx: mpsc::Receiver<WorkerMessage>,
    metrics: Arc<Mutex<Metrics>>,
) -> Vec<thread::JoinHandle<()>> {
    let shared_rx = Arc::new(Mutex::new(rx));
    let mut handles = Vec::with_capacity(worker_count);

    for _ in 0..worker_count {
        let rx_clone = Arc::clone(&shared_rx);
        let metrics_clone = Arc::clone(&metrics);

        let handle = thread::spawn(move || loop {
            let message = {
                let guard = rx_clone.lock().unwrap();
                guard.recv()
            };

            match message {
                Ok(WorkerMessage::Task(task)) => {
                    let start = Instant::now();
                    match task.kind {
                        TaskType::CPU => {
                            let complete_at = Instant::now() + task.duration;
                            while Instant::now() < complete_at {}
                        }
                        TaskType::IO => {
                            thread::sleep(task.duration);
                        }
                    }
                    let finish = Instant::now();
                    let mut metrics_guard = metrics_clone.lock().unwrap();
                    metrics_guard.record(task, start, finish);
                }
                Ok(WorkerMessage::Shutdown) | Err(_) => break,
            }
        });

        handles.push(handle);
    }

    handles
}

pub fn report_metrics(
    experiment_name: &str,
    _total_tasks: usize,
    _worker_count: usize,
    _cpu_probability: f64,
    _seed: u64,
    output_path: &str,
    metrics: &Metrics,
) -> io::Result<()> {
    let output = format!("\n=== {} ===\n{}", experiment_name, metrics.render());

    print!("{}", output);
    if let Some(parent) = std::path::Path::new(output_path).parent() {
        create_dir_all(parent)?;
    }

    let mut file = File::create(output_path)?;
    file.write_all(output.as_bytes())?;
    Ok(())
}

struct ExperimentSpec {
    name: &'static str,
    cpu_probability: f64,
    seed: u64,
    total_tasks: usize,
    results_file: &'static str,
}

fn main() {
    run_experiment(ExperimentSpec {
        name: "Balanced workload (50/50 CPU/IO)",
        cpu_probability: 0.5,
        seed: 42,
        total_tasks: 1000,
        results_file: "results/balance.txt",
    });

    run_experiment(ExperimentSpec {
        name: "CPU-heavy workload (80/20)",
        cpu_probability: 0.8,
        seed: 99,
        total_tasks: 1000,
        results_file: "results/cpu.txt",
    });
}

fn run_experiment(spec: ExperimentSpec) {
    let worker_count = 8;
    let (gen_tx, gen_rx) = mpsc::channel();
    let (worker_tx, worker_rx) = mpsc::channel::<WorkerMessage>();
    let metrics = Arc::new(Mutex::new(Metrics::new(worker_count)));

    let generator_handle = start_generator(gen_tx, spec.total_tasks, spec.cpu_probability, spec.seed);
    let dispatcher_handle = start_dispatcher(gen_rx, worker_tx, worker_count, Arc::clone(&metrics));
    let worker_handles = start_workers(worker_count, worker_rx, Arc::clone(&metrics));

    generator_handle.join().expect("generator thread failed");
    dispatcher_handle.join().expect("dispatcher thread failed");
    for handle in worker_handles {
        handle.join().expect("worker thread failed");
    }

    let metrics_guard = metrics.lock().unwrap();
    report_metrics(
        spec.name,
        spec.total_tasks,
        worker_count,
        spec.cpu_probability,
        spec.seed,
        spec.results_file,
        &metrics_guard,
    )
    .expect("failed to write experiment results");
}
