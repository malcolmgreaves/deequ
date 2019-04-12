package sparktest.util.sync

import java.util.concurrent.locks.ReentrantReadWriteLock

/**
  * A WaitGroup is a data structure used to synchronize many concurrently
  * executing operations, where the number of executing operations is
  * only known at runtime. This constraint precludes the use of a
  * Java concurrent CountDownLatch as the maximum number of concurrent
  * operations must be specified at compile time.
  *
  * Inspiration for this class, including its API, was motivated by the
  * WaitGroup struct in Go: http://golang.org/pkg/sync/#WaitGroup
  *
  * NOTE, design decision:
  * Chose as final class since it's highly mutable and shouldn't be mixed-in
  * nor extended. A `WaitGroup` has a incredibly narrow, well-defined purpose.
  * It should not be used for anything else.
  */
final class WaitGroup {

  // we want fair locking so the constructor param is true
  private[this] val lk = new ReentrantReadWriteLock(true)

  private[this] var counter = 0

  /**
    * BLOCKING OPERATION.
    *
    * Register `k` operations with the wait group. Safely increments
    * the internal counter by `k`, meaning that we expect a `done(k)`
    * call or `k` `done(1)` calls in the future.
    *
    * Blocking must wait for internal write lock.
    */
  def add(k: Int): Unit = {
    try {
      lk.writeLock.lock()
      counter += k

    } finally {
      lk.writeLock.unlock()
    }
  }

  /**
    * BLOCKING OPERATION
    *
    * Acknowledge that `k` previosuly registered operations have completed.
    * Safely decrements the internal counter by `k`, meaning that we are
    * accounting for a `add(k)` call or `k` `add(1)` calls in the past.
    *
    * This method throws an `IllegalStateException` if we one calls `done`
    * more than one has called `add`. I.e. an exception is thrown if the
    * internal counter ever becomes negative.
    *
    * Blocking must wait for internal write lock.
    */
  def done(k: Int): Unit = {
    try {
      lk.writeLock.lock()
      counter -= k
      if (counter < 0) {
        throw new IllegalStateException("Can never call add more than done!")
      }

    } finally {
      lk.writeLock.unlock()
    }
  }

  type Millisecond = Int

  /**
    * BLOCKING OPERATION
    *
    * Blocks until the internal counter is zero. The semantics here are that an
    * executing thread will wait for all other threads "in" the waiting group
    * to finish before it can proceed. This method sleeps for `sleepIncrement`
    * milliseconds in-between each counter check. By default the wait is 1.3
    * seconds. We suggest that one does not have too short of a wait time,
    * otherwise one risks a busy wait loop!
    *
    * Blocking must wait for internal read lock.
    */
  def waitUntilCompletion(sleepIncrement: Millisecond = 1300): Unit =
    while (!counterIsZero) {
      Thread.sleep(sleepIncrement)
    }

  private[this] def counterIsZero: Boolean = {
    try {
      lk.readLock.lock()
      counter == 0
    } finally {
      lk.readLock.unlock()
    }
  }

}
