//! Threads that run rounds of a benchmark together, timed by the thread that tells them to.

use std::{
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::cpus::Cpus;

/// What one thread of a [`Crew`] runs in each round.
pub type Round = Box<dyn FnMut() + Send>;

/// A thread for each of a set of [`Round`]s, which runs its round each time [`Crew::time`] says
/// to.
///
/// The threads are spawned once and kept for every sample, so that spawning and joining them is
/// never timed: a pair of barriers starts a round and waits for it to end, and only the time
/// between the two is measured.
///
/// Each thread is pinned to a CPU of its own, for the reasons [`crate::cpus`] gives, and so is the
/// thread that times them, for as long as it does.
pub struct Crew {
    cpus: Cpus,
    stop: Arc<AtomicBool>,
    start: Arc<Barrier>,
    end: Arc<Barrier>,
    threads: Vec<JoinHandle<()>>,
}

impl Crew {
    /// Spawns a thread for each of `rounds`, pinned to the CPU of the same index in the order the
    /// calling thread may run on them.
    pub fn spawn(rounds: impl IntoIterator<Item = Round>) -> Self {
        let rounds: Vec<_> = rounds.into_iter().collect();
        let count = rounds.len();
        let cpus = Cpus::allowed();
        let stop = Arc::new(AtomicBool::new(false));
        // The crew, and the thread that times it.
        let start = Arc::new(Barrier::new(count + 1));
        let end = Arc::new(Barrier::new(count + 1));

        let threads = rounds
            .into_iter()
            .enumerate()
            .map(|(index, mut round)| {
                let cpu = cpus.nth(index);
                let stop = stop.clone();
                let start = start.clone();
                let end = end.clone();

                thread::spawn(move || {
                    let _pinned = cpu.pin();
                    loop {
                        start.wait();
                        if stop.load(Ordering::Acquire) {
                            return;
                        }
                        round();
                        end.wait();
                    }
                })
            })
            .collect();

        Self {
            cpus,
            stop,
            start,
            end,
            threads,
        }
    }

    /// Runs `iters` rounds, one after the other, and the time they took, each from the crew
    /// being told to start it to the last of the crew being done with it.
    pub fn time(&self, iters: u64) -> Duration {
        let _pinned = self.cpus.nth(self.threads.len()).pin();
        let mut total = Duration::ZERO;
        for _ in 0..iters {
            let started = Instant::now();
            self.start.wait();
            self.end.wait();
            total += started.elapsed();
        }

        total
    }

    /// Lets the threads go with `stop` set, so that they exit rather than run a round, and
    /// joins them.
    pub fn join(self) {
        self.stop.store(true, Ordering::Release);
        self.start.wait();
        for thread in self.threads {
            thread.join().unwrap();
        }
    }
}
