/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.Striped;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.hddsfs.HDDSFSConfigKeys;
import org.apache.hadoop.hdfs.server.lock.collections.LockPool;
import org.apache.hadoop.hdfs.server.lock.exception.ExceptionMessage;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.LockMode;
import org.apache.hadoop.hdfs.server.lock.resource.LockResource;
import org.apache.hadoop.hdfs.server.lock.resource.RWLockResource;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InodeLockManager implements Closeable {
  /**
   * Pool for supplying inode locks. To lock an inode, its inode id must be searched in this
   * pool to get the appropriate read lock.
   *
   * We use weak values so that when nothing holds a reference to
   * a lock, the garbage collector can remove the lock's entry from the pool.
   */
  private final LockPool<Long> mInodeLocks;

  /**
   * Locks for guarding changes to last modified time and size on read-locked parent inodes.
   *
   * When renaming, creating, or deleting, we update the last modified time, last access time
   * and size of the parent inode while holding only a read lock. In the presence of concurrent
   * operations, this could cause the last modified time to decrease, or lead to incorrect
   * directory sizes. To avoid this, we guard the parent inode read-modify-write with this lock.
   * To avoid deadlock, a thread should never acquire more than one of these locks at the same time,
   * and no other locks should be taken while holding one of these locks.
   */
  private final Striped<Lock> mParentUpdateLocks = Striped.lock(1_000);

  /**
   * Cache for supplying inode persistence locks. Before a thread can persist an inode, it must
   * acquire the persisting lock for the inode. The cache maps inode ids to AtomicBooleans used to
   * provide mutual exclusion for inode persisting threads.
   */
  private final LoadingCache<Long, AtomicBoolean> mPersistingLocks =
      CacheBuilder.newBuilder()
          .weakValues()
          .initialCapacity(1_000)
          .concurrencyLevel(100)
          .build(new CacheLoader<Long, AtomicBoolean>() {
            @Override
            public AtomicBoolean load(Long key) {
              return new AtomicBoolean();
            }
          });

  public InodeLockManager(Configuration conf) {
    int initSize =
        conf.getInt(HDDSFSConfigKeys.HDDSFS_NAMENODE_LOCK_POOL_INITSIZE_KEY,
            HDDSFSConfigKeys.HDDSFS_NAMENODE_LOCK_POOL_INITSIZE_KEY_DEFAULT);
    int lowWatermark =
        conf.getInt(HDDSFSConfigKeys.HDDSFS_NAMENODE_LOCK_POOL_LOW_WATERMARK_KEY,
            HDDSFSConfigKeys.HDDSFS_NAMENODE_LOCK_POOL_LOW_WATERMARK_DEFAULT);
    int highWatermark =
        conf.getInt(HDDSFSConfigKeys.HDDSFS_NAMENODE_LOCK_POOL_HIGH_WATERMARK_KEY,
            HDDSFSConfigKeys.HDDSFS_NAMENODE_LOCK_POOL_HIGH_WATERMARK_DEFAULT);
    int concurrencyLevel =
        conf.getInt(HDDSFSConfigKeys.HDDSFS_NAMENODE_LOCK_POOL_CONCURRENCY_LEVEL_KEY,
            HDDSFSConfigKeys.HDDSFS_NAMENODE_LOCK_POOL_CONCURRENCY_LEVEL_DEFAULT);
    mInodeLocks =
        new LockPool<>((key)-> new ReentrantReadWriteLock(),
            initSize,
            lowWatermark,
            highWatermark,
            concurrencyLevel);
  }

  @VisibleForTesting
  boolean inodeReadLockedByCurrentThread(long inodeId) {
    return mInodeLocks.getRawReadWriteLock(inodeId).getReadHoldCount() > 0;
  }

  @VisibleForTesting
  boolean inodeWriteLockedByCurrentThread(long inodeId) {
    return mInodeLocks.getRawReadWriteLock(inodeId).getWriteHoldCount() > 0;
  }

  /**
   * Asserts that all locks have been released, throwing an exception if any locks are still taken.
   */
  @VisibleForTesting
  public void assertAllLocksReleased() {
    assertAllLocksReleased(mInodeLocks);
  }

  private <T> void assertAllLocksReleased(LockPool<T> pool) {
    for (Map.Entry<T, ReentrantReadWriteLock> entry : pool.getEntryMap().entrySet()) {
      ReentrantReadWriteLock lock = entry.getValue();
      if (lock.isWriteLocked()) {
        throw new RuntimeException(
            String.format("Found a write-locked lock for %s", entry.getKey()));
      }
      if (lock.getReadLockCount() > 0) {
        throw new RuntimeException(
            String.format("Found a read-locked lock for %s", entry.getKey()));
      }
    }
  }

  /**
   * Acquires an inode lock.
   *
   * @param inode the inode to lock
   * @param mode the mode to lock in
   * @param useTryLock whether to acquire with {@link Lock#tryLock()} or {@link Lock#lock()}. This
   *                   method differs from {@link #tryLockInode(Long, LockMode)} because it will
   *                   block until the inode has been successfully locked.
   * @return a lock resource which must be closed to release the lock
   * @see #tryLockInode(Long, LockMode)
   */
  public RWLockResource lockInode(INode inode, LockMode mode, boolean useTryLock) {
    return mInodeLocks.get(inode.getId(), mode, useTryLock);
  }

  /**
   * Attempts to acquire an inode lock.
   *
   * @param inodeId the inode id to try locking
   * @param mode the mode to lock in
   * @return either an empty optional, or a lock resource which must be closed to release the lock
   */
  public Optional<RWLockResource> tryLockInode(Long inodeId, LockMode mode) {
    return mInodeLocks.tryGet(inodeId, mode);
  }

  /**
   * Tries to acquire a lock for persisting the specified inode id.
   *
   * @param inodeId the inode to acquire the lock for
   * @return an optional wrapping a closure for releasing the lock on success, or Optional.empty if
   *         the lock is already taken
   */
  public Optional<Closeable> tryAcquirePersistingLock(long inodeId) {
    AtomicBoolean lock = mPersistingLocks.getUnchecked(inodeId);
    if (lock.compareAndSet(false, true)) {
      return Optional.of(() -> lock.set(false));
    }
    return Optional.empty();
  }

  /**
   * Acquires the lock for modifying an inode's last modified time or size. As a pre-requisite, the
   * current thread should already hold a read lock on the inode.
   *
   * @param inodeId the id of the inode to lock
   * @return a lock resource which must be closed to release the lock
   */
  public LockResource lockUpdate(long inodeId) {
    return new LockResource(mParentUpdateLocks.get(inodeId));
  }



  /**
   * Obtains a read lock on the inode. Afterward, checks the inode state to ensure the full inode
   * path is consistent with what the caller is expecting. If the state is inconsistent, an
   * exception will be thrown and the lock will be released.
   *
   * NOTE: This method assumes that the inode path to the parent has been read locked.
   *
   * @param parent the expected parent inode
   * @param name the expected name of the inode to be locked
   * @throws InvalidPathException if the parent and/or name is not as expected
   */
  public RWLockResource lockReadAndCheckNameAndParent(INode inode,
      boolean useTryLock, INode parent, byte[] name) throws InvalidPathException {
    RWLockResource lockResource = lockReadAndCheckParent(inode, useTryLock, parent);
    if (!Arrays.equals(inode.getLocalNameBytes(), name)) {
      lockResource.close();
      throw new InvalidPathException(ExceptionMessage.PATH_INVALID_CONCURRENT_RENAME.getMessage());
    }
    return lockResource;
  }

  /**
   * Obtains a read lock on the inode. Afterward, checks the inode state:
   *   - parent is consistent with what the caller is expecting
   *   - the inode is not marked as deleted
   * If the state is inconsistent, an exception will be thrown and the lock will be released.
   *
   * NOTE: This method assumes that the inode path to the parent has been read locked.
   *
   * @param parent the expected parent inode
   * @throws InvalidPathException if the parent is not as expected
   */
  public RWLockResource lockReadAndCheckParent(INode inode,
      boolean useTryLock, INode parent) throws InvalidPathException {
    RWLockResource lockResource = lockInode(inode, LockMode.READ, useTryLock);
    if (inode.isDeleted()) {
      lockResource.close();
      throw new InvalidPathException(ExceptionMessage.PATH_INVALID_CONCURRENT_DELETE.getMessage());
    }
    if (parent != null && inode.getParent() != null
        && parent.getId() != inode.getParent().getId()) {
      lockResource.close();
      throw new InvalidPathException(ExceptionMessage.PATH_INVALID_CONCURRENT_RENAME.getMessage());
    }
    return lockResource;
  }

  /**
   * Obtains a write lock on the inode. Afterward, checks the inode state:
   *   - parent is consistent with what the caller is expecting
   *   - the inode is not marked as deleted
   * If the state is inconsistent, an exception will be thrown and the lock will be released.
   *
   * NOTE: This method assumes that the inode path to the parent has been read locked.
   *
   * @param parent the expected parent inode
   * @throws InvalidPathException if the parent is not as expected
   */
  public RWLockResource lockWriteAndCheckParent(INode inode,
      boolean useTryLock, INode parent) throws InvalidPathException {
    RWLockResource lockResource = lockInode(inode, LockMode.WRITE, useTryLock);
    if (inode.isDeleted()) {
      lockResource.close();
      throw new InvalidPathException(ExceptionMessage.PATH_INVALID_CONCURRENT_DELETE.getMessage());
    }
    if (parent != null && inode.getParent() != null
        && parent.getId() != inode.getParent().getId()) {
      lockResource.close();
      throw new InvalidPathException(ExceptionMessage.PATH_INVALID_CONCURRENT_RENAME.getMessage());
    }
    return lockResource;
  }

  /**
   * Obtains a write lock on the inode. Afterward, checks the inode state to ensure the full inode
   * path is consistent with what the caller is expecting. If the state is inconsistent, an
   * exception will be thrown and the lock will be released.
   *
   * NOTE: This method assumes that the inode path to the parent has been read locked.
   *
   * @param parent the expected parent inode
   * @param name the expected name of the inode to be locked
   * @throws InvalidPathException if the parent and/or name is not as expected
   */
  public RWLockResource lockWriteAndCheckNameAndParent(INode inode,
      boolean useTryLock, INode parent, byte[] name)
      throws InvalidPathException {
    RWLockResource lockResource = lockWriteAndCheckParent(inode, useTryLock, parent);
    if (!Arrays.equals(inode.getLocalNameBytes(), name)) {
      lockResource.close();
      throw new InvalidPathException(ExceptionMessage.PATH_INVALID_CONCURRENT_RENAME.getMessage());
    }
    return lockResource;
  }

  @Override
  public void close() throws IOException {
    mInodeLocks.close();
  }

  public int getInodeLockPoolSize() {
    return mInodeLocks.size();
  }
}
