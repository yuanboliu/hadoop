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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.server.lock.concurrent.LockMode;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Reference counted Lock resource, automatically unlocks and decrements the reference count.
 * It contains a lock and a reference count for that lock, and will decrement
 * the lock reference count and unlocking when the resource is closed.
 */
public class RefCountLockResource extends RWLockResource {
  private final AtomicInteger mRefCount;

  /**
   * Creates a new instance of {@link LockResource} using the given lock and reference counter. The
   * reference counter should have been initialized and incremented outside of this class.
   *
   * @param lock the lock to acquire
   * @param mode the mode to acquire the lock in
   * @param acquireLock whether to lock the lock
   * @param refCount ref count for the lock
   * @param useTryLock applicable only if acquireLock is true. Determines whether or not to use
   *                   {@link Lock#tryLock()} or {@link Lock#lock()} to acquire the lock
   */
  public RefCountLockResource(ReentrantReadWriteLock lock, LockMode mode, boolean acquireLock,
      AtomicInteger refCount, boolean useTryLock) {
    super(lock, mode, acquireLock, useTryLock);
    mRefCount = Preconditions
        .checkNotNull(refCount, "Reference Counter can not be null");
  }

  /**
   * Releases the lock and decrement the ref count if a ref counter was provided
   * at construction time.
   */
  @Override
  public void close() {
    super.close();
    mRefCount.decrementAndGet();
  }
}
