/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark;

import com.oltpbenchmark.types.State;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to share a state among the workers of a single workload. Worker use it to ask
 * for work and as interface to the global BenchmarkState
 *
 * @author alendit
 */
public class WorkloadState {
  private static final int RATE_QUEUE_LIMIT = 10000;
  private static final Logger LOG = LoggerFactory.getLogger(WorkloadState.class);

  private final BenchmarkState benchmarkState;
  private final LinkedList<SubmittedProcedure> workQueue = new LinkedList<>();
  private final int num_terminals;
  private final Iterator<Phase> phaseIterator;

  private int workersWaiting = 0;

  // volatile so the per-transaction fast paths (stayAwake, fetchWork on
  // unlimited-rate phases) can check it without taking the monitor: with
  // 512-1024 terminals, any per-transaction acquisition of this single
  // monitor convoys every worker AND starves ThreadBench's main loop
  // (addToQueue), which is what actually ends the phase on time.
  private volatile int workerNeedSleep;

  private volatile Phase currentPhase = null;

  public WorkloadState(BenchmarkState benchmarkState, List<Phase> works, int num_terminals) {
    this.benchmarkState = benchmarkState;
    this.num_terminals = num_terminals;
    this.workerNeedSleep = num_terminals;

    phaseIterator = works.iterator();
  }

  /** Add a request to do work. */
  public void addToQueue(int amount, boolean resetQueues) {
    // Lock-free fast path: unlimited-rate phases never use the work queue, and
    // this method is called from ThreadBench's main loop every tick. Without
    // this check the main loop queues behind every worker on the monitor and
    // cannot end the phase on schedule.
    if (!resetQueues) {
      Phase phase = currentPhase;
      if (phase == null || phase.isDisabled() || !phase.isRateLimited() || phase.isSerial()) {
        return;
      }
    }

    int workAdded = 0;

    synchronized (this) {
      if (resetQueues) {
        workQueue.clear();
      }

      // Only use the work queue if the phase is enabled and rate limited.
      if (currentPhase == null
          || currentPhase.isDisabled()
          || !currentPhase.isRateLimited()
          || currentPhase.isSerial()) {
        return;
      }

      // Add the specified number of procedures to the end of the queue.
      // If we can't keep up with current rate, truncate transactions
      for (int i = 0; i < amount && workQueue.size() <= RATE_QUEUE_LIMIT; ++i) {
        workQueue.add(new SubmittedProcedure(currentPhase.chooseTransaction()));
        workAdded++;
      }

      // Wake up sleeping workers to deal with the new work.
      int numToWake = Math.min(workAdded, workersWaiting);
      while (numToWake-- > 0) {
        this.notify();
      }
    }
  }

  public void signalDone() {
    int current = this.benchmarkState.signalDone();
    if (current == 0) {
      synchronized (this) {
        if (workersWaiting > 0) {
          this.notifyAll();
        }
      }
    }
  }

  /** Called by ThreadPoolThreads when waiting for work. */
  public SubmittedProcedure fetchWork() {
    // Read the volatile phase once; using the snapshot below also removes the
    // window where a concurrent phase switch could null currentPhase between
    // the branch check and its use.
    Phase phase = currentPhase;

    if (phase != null && phase.isSerial()) {
      synchronized (this) {
        ++workersWaiting;
        while (getGlobalState() == State.LATENCY_COMPLETE) {
          try {
            this.wait();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        --workersWaiting;

        if (getGlobalState() == State.EXIT || getGlobalState() == State.DONE) {
          return null;
        }

        // Re-read the phase: the wait above can span a phase switch, and the
        // serial cursor must come from the phase that is current now.
        Phase phaseAfterWait = currentPhase;
        if (phaseAfterWait == null) {
          return null;
        }
        return new SubmittedProcedure(
            phaseAfterWait.chooseTransaction(getGlobalState() == State.COLD_QUERY));
      }
    }

    // Unlimited-rate phases don't use the work queue; nothing here needs the
    // monitor (the old synchronized block only maintained a never-read
    // counter and convoyed all workers at high terminal counts).
    if (phase != null && !phase.isRateLimited()) {
      return new SubmittedProcedure(phase.chooseTransaction(getGlobalState() == State.COLD_QUERY));
    }

    synchronized (this) {
      // Sleep until work is available.
      if (workQueue.peek() == null) {
        workersWaiting += 1;
        while (workQueue.peek() == null) {
          if (this.benchmarkState.getState() == State.EXIT
              || this.benchmarkState.getState() == State.DONE) {
            return null;
          }

          try {
            this.wait();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        workersWaiting -= 1;
      }

      return workQueue.remove();
    }
  }

  public void finishedWork() {
    // Kept for API symmetry with fetchWork; the workersWorking counter it
    // maintained was never read, and taking the shared monitor here once per
    // transaction convoyed every worker.
  }

  public Phase getNextPhase() {
    if (phaseIterator.hasNext()) {
      return phaseIterator.next();
    }
    return null;
  }

  public Phase getCurrentPhase() {
    // currentPhase is volatile and written under the WorkloadState monitor in
    // switchToNextPhase(); the old synchronized(benchmarkState) here locked an
    // unrelated object (not the writer's lock) and convoyed all workers.
    return currentPhase;
  }

  /*
   * Called by workers to ask if they should stay awake in this phase
   */
  public void stayAwake() {
    // Lock-free fast path for the common case (phase active, nobody needs to
    // sleep). workerNeedSleep is volatile; a worker that races a phase switch
    // and misses the new sleep request re-checks on its next loop iteration,
    // which is the same guarantee the fully-locked version gave between
    // consecutive transactions.
    if (workerNeedSleep == 0) {
      return;
    }
    synchronized (this) {
      while (workerNeedSleep > 0) {
        workerNeedSleep--;
        try {
          this.wait();
        } catch (InterruptedException e) {
          LOG.error(e.getMessage(), e);
        }
      }
    }
  }

  public void switchToNextPhase() {
    synchronized (this) {
      this.currentPhase = this.getNextPhase();

      // Clear the work from the previous phase.
      workQueue.clear();

      // Determine how many workers need to sleep, then make sure they
      // do.
      if (this.currentPhase == null)
      // Benchmark is over---wake everyone up so they can terminate
      {
        workerNeedSleep = 0;
      } else {
        this.currentPhase.resetSerial();
        if (this.currentPhase.isDisabled())
        // Phase disabled---everyone should sleep
        {
          workerNeedSleep = this.num_terminals;
        } else
        // Phase running---activate the appropriate # of terminals
        {
          workerNeedSleep = this.num_terminals - this.currentPhase.getActiveTerminals();
        }
      }

      this.notifyAll();
    }
  }

  /** Delegates pre-start blocking to the global state handler */
  public void blockForStart() {
    benchmarkState.blockForStart();
  }

  /**
   * Delegates a global state query to the benchmark state handler
   *
   * @return global state
   */
  public State getGlobalState() {
    return benchmarkState.getState();
  }

  public void signalLatencyComplete() {

    benchmarkState.signalLatencyComplete();
  }

  public void startColdQuery() {

    benchmarkState.startColdQuery();
  }

  public void startHotQuery() {

    benchmarkState.startHotQuery();
  }

  public long getTestStartNs() {
    return benchmarkState.getTestStartNs();
  }
}
