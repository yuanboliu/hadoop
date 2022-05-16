/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdfs.server.lock.resource;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;

/**
 * A resource lock that makes it possible to acquire and release locks using the following idiom:
 *
 * <pre>
 *   try (LockResource r = new LockResource(lock)) {
 *     ...
 *   }
 * </pre>
 */
// extends Closeable instead of AutoCloseable to enable usage with Guava's Closer.
public class LockResource implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(LockResource.class);

  // The lock which represents the resource. It should only be written or modified by subclasses
  // attempting to downgrade locks (see RWLockResource).
  protected Lock mLock;
  private final Runnable mCloseAction;

  /**
   * Creates a new instance of {@link LockResource} using the given lock.
   *
   * @param lock the lock to acquire
   */
  public LockResource(Lock lock) {
    this(lock, true, false);
  }

  /**
   * Creates a new instance of {@link LockResource} using the given lock.
   *
   * This method may use the {@link Lock#tryLock()} method to gain ownership of the locks. The
   * reason one might want to use this is to avoid the fairness heuristics within the
   * {@link java.util.concurrent.locks.ReentrantReadWriteLock}'s NonFairSync which may block reader
   * threads if a writer if the first in the queue.
   *
   * @param lock the lock to acquire
   * @param acquireLock whether to lock the lock
   * @param useTryLock whether or not use to {@link Lock#tryLock()}
   */
  public LockResource(Lock lock, boolean acquireLock, boolean useTryLock) {
    this(lock, acquireLock, useTryLock, null);
  }

  /**
   * Creates a new instance of {@link LockResource} using the given lock.
   *
   * This method may use the {@link Lock#tryLock()} method to gain ownership of the locks. The
   * reason one might want to use this is to avoid the fairness heuristics within the
   * {@link java.util.concurrent.locks.ReentrantReadWriteLock}'s NonFairSync which may block reader
   * threads if a writer if the first in the queue.
   *
   * @param lock the lock to acquire
   * @param acquireLock whether to lock the lock
   * @param useTryLock whether or not use to {@link Lock#tryLock()}
   * @param closeAction the nullable closeable that will be run before releasing the lock
   */
  public LockResource(Lock lock, boolean acquireLock, boolean useTryLock,
      @Nullable Runnable closeAction) {
    mLock = lock;
    mCloseAction = closeAction;
    if (acquireLock) {
      if (useTryLock) {
        while (!mLock.tryLock()) { // returns immediately
          // The reason we don't use #tryLock(int, TimeUnit) here is because we found there is a bug
          // somewhere in the internal accounting of the ReentrantRWLock that, even though all
          // threads had released the lock, that a final thread would never be able to acquire it.
          LockSupport.parkNanos(10000);
        }
      } else {
        mLock.lock();
      }
    }
  }

  /**
   * Returns true if the other {@link LockResource} contains the same lock.
   *
   * @param other other LockResource
   * @return true if the other lockResource has the same lock
   */
  @VisibleForTesting
  public boolean hasSameLock(LockResource other) {
    return mLock == other.mLock;
  }

  /**
   * Releases the lock.
   */
  @Override
  public void close() {
    if (mCloseAction != null) {
      mCloseAction.run();
    }
    mLock.unlock();
  }
}
