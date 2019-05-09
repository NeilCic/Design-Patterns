package threadPool;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import waitablePQ.WaitablePQ;


public class ThreadPool {
	private volatile boolean isPaused = false;
	private volatile int numOfThreads;
	private final WaitablePQ<Task<?>> workQ = new WaitablePQ<>(((t1, t2) -> t1.p.getNumVal() - t2.p.getNumVal()));
	private Semaphore termination = new Semaphore(0);
	private volatile boolean threadpoolActive = true;
	private Object pauser = new Object();
	private Semaphore pauseSem = new Semaphore(0);

	public ThreadPool(int numOfThread) {
		this.numOfThreads = numOfThread; // first batch of threads created only at execution. 0 threads made at creation of pool.
	}

	private void addThreads(int delta) {
		for (int i = 0; i < delta; ++i) {
			new ExecutingThread().start();
		}
	}

	public void executePool() {
		addThreads(numOfThreads);
	}
	
	private class ExecutingThread extends Thread {
		private boolean active = true;
		
		@Override
		public void run() {
			while (active) {
				try {
					workQ.dequeue().taskRun();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private static class Task<T> {
		private final Callable<T> callable;
		private final Priority p;
		private MyFuture f = new MyFuture();
		private Object synchronizer = new Object();
		
		private Task(Callable<T> callable, Priority p) {
			this.callable = callable;
			this.p = p;
		}
		
		private void taskRun() {
			if (!f.isCancelled() && !f.isDone()) {
				try {
					f.setResult(callable.call());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		private class MyFuture implements Future<T> {
			private T result;
			private Status status;

			private void setResult(T res) {
				f.result = res;
				f.status = Status.DONE;
				synchronized(synchronizer) {
					synchronizer.notify(); //notifying the get() method.
				}
			}
			
			@Override
			public boolean cancel(boolean b) {
				if (this.status == Status.WAITING) {
					synchronized(synchronizer) {
						if (this.status == Status.WAITING) {
							this.status = Status.CANCELLED; // the thread DQs the task and taskRun() skips the task since its status is cancelled.
						}
					}
					
					return true;
				}
				
				return false;
			}

			@Override
			public boolean isCancelled() {
				return status == Status.CANCELLED;
			}

			@Override
			public boolean isDone() {
				return status == Status.DONE;
			}

			@Override
			public T get() throws InterruptedException, ExecutionException {
				if (!isDone()) {
					synchronized(synchronizer) {
						if (!isDone()) {
							synchronizer.wait();
						}
					}
				}
				
				return result;
			}

			@Override
			public T get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
				if (!isDone()) {
					synchronized(synchronizer) {
						if (!isDone()) {
							synchronizer.wait(TimeUnit.MILLISECONDS.convert(l, timeUnit));
							if (!isDone()) {
								throw new TimeoutException();
							}
						}
					}
				}
				
				return result;
			}
		}
	}

	private interface Priority {
		public int getNumVal();
	}
	
	public enum UserPriority implements Priority {
		MIN(1), DEFAULT(5), MAX(10);

		private final int numVal;

		UserPriority(int numVal) {
			this.numVal = numVal;
		}

		@Override
		public int getNumVal() {
			return numVal;
		}
	}
	
	private enum SysPriority implements Priority {
		SHUTDOWN(0),
		PAUSE(11);
		
		private final int numVal;

		SysPriority(int numVal) {
			this.numVal = numVal;
		}

		@Override
		public int getNumVal() {
			return numVal;
		}
	}

	public enum Status {
		DONE,
		CANCELLED,
		WAITING;
	}

	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		return termination.tryAcquire(numOfThreads, timeout, unit);
	}

	public void shutdown() {
		threadpoolActive = false;
		reduceThreads(numOfThreads);
	}
	
	public void pause() {
		synchronized (pauser) {
			if (isPaused == false ) {
				isPaused = true;
				for (int i = 0; i < numOfThreads; ++i) {
					submit(() -> {
						try {
							pauseSem.acquire();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}, SysPriority.PAUSE);
				}
			}
		}
	}

	public void resume() {
		synchronized (pauser) {
			if (isPaused == true) {
				isPaused = false;
				pauseSem.release(numOfThreads);
			}
		}
	}
	
	public void setNumOfThreads(int numOfThreads) {
		if (numOfThreads > this.numOfThreads) {
			addThreads(numOfThreads - this.numOfThreads);
		} else {
			reduceThreads(numOfThreads - this.numOfThreads);
		}
		this.numOfThreads = numOfThreads;
	}

	private void reduceThreads(int delta) {
		for (int i = 0; i < delta; ++i) {
			submit(() -> {
				((ExecutingThread)Thread.currentThread()).active = false;
				termination.release(); // adding a permit so that awaiteTermination() won't stall over nothing.
			}, SysPriority.PAUSE);
		}
	}
	
	public Future<Void> submit(Runnable task, Priority priority) {
		return submit(task, priority, null);
	}

	public <T> Future<T> submit(Runnable task, Priority priority, T value) {
		return submit(Executors.callable(task, value), priority);
	}

	public <T> Future<T> submit(Callable<T> call, Priority priority) {
		Objects.requireNonNull(call, "submitted null callable");
		Objects.requireNonNull(priority, "submitted null priority");
		if (!threadpoolActive) {
			throw new RejectedExecutionException("submitted task to shutdown threadpool.");
		}
		Task<T> task = new Task<>(call, priority);
		workQ.enqueue(task);
		
		return task.f;
	}

	public <T> Future<T> submit(Callable<T> call) {
		return submit(call, UserPriority.DEFAULT);
	}
	
	public static void main(String[] args) {
        int i = 0;

        while (i < 10) {
            ThreadPool tp = new ThreadPool(2);

            tp.submit(() -> {
                System.out.println("hello Regular priority");
                return 0;
            });
            tp.submit(() -> {
                System.out.println("hello Max priority");
                return 0;
            }, UserPriority.MAX);

            ++i;
            tp.executePool();
            tp.shutdown();
            try {
                tp.awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
