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

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.hdfs.server.lock.resource.RWLockResource;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.LockMode;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;

/**
 * Manages the locks for a list of {@link INode}.
 */
@ThreadSafe
public class InodeLockList implements AutoCloseable {
  protected List<INode> mInodes;
  /**
   * Lock list.
   * The locks always alternate between Inode lock and Edge lock.
   * The first lock can be either Inode or Edge lock.
   */
  protected LinkedList<RWLockResource> mLocks;
  /** Whether to use {@link Lock#tryLock()} or {@link Lock#lock()}. */
  private final boolean mUseTryLock;
  private final InodeLockManager mInodeLockManager;
  public static final InodeLockList emptyInodeLockList =
      new InodeLockList(null, false);

  /**
   * Creates a new instance of {@link InodeLockList}.
   */
  public InodeLockList(InodeLockManager inodeLockManager, boolean useTryLock) {
    mInodes = new ArrayList<>();
    mLocks = new LinkedList<>();
    mInodeLockManager = inodeLockManager;
    mUseTryLock = useTryLock;
  }

  /**
   * Creates a new instance of {@link InodeLockList}.
   */
  public InodeLockList(List<INode> inodes, LinkedList<RWLockResource> locks,
      InodeLockManager inodeLockManager, boolean useTryLock) {
    mInodes = inodes;
    mLocks = locks;
    mInodeLockManager = inodeLockManager;
    mUseTryLock = useTryLock;
  }

  /**
   * Locks the given inode in read mode, and adds it to this lock list. This call should only be
   * used when locking the root or an inode by id and not path or parent.
   *
   * @param inode the inode to lock
   */
  public synchronized void lockRead(INode inode) {
    mLocks.add(mInodeLockManager.lockInode(inode, LockMode.READ, mUseTryLock));
    mInodes.add(inode);
  }

  InodeLockList getAncestorINodeLockListInPath(int length) {
    List<INode> inodes = new ArrayList<>();
    LinkedList<RWLockResource> locks = new LinkedList<>();
    for (int i = 0; i < length && i < mInodes.size(); i ++) {
      inodes.add(mInodes.get(i));
      locks.add(mLocks.get(i));
    }
    return new InodeLockList(inodes, locks, mInodeLockManager, false);
  }

  /**
   * Locks the given inode in read mode, and adds it to this lock list. This method ensures the
   * parent is the expected parent inode.
   *
   * NOTE: This method assumes that the inode path to the parent has been read locked.
   *
   * @param inode the inode to lock
   * @param parent the expected parent inode
   * @throws InvalidPathException if the inode is no long consistent with the caller's expectations
   */
  public synchronized void lockReadAndCheckParent(INode inode, INode parent)
      throws InvalidPathException {
    mLocks.add(mInodeLockManager.lockReadAndCheckParent(inode, mUseTryLock, parent));
    mInodes.add(inode);
  }

  /**
   * Locks the given inode in read mode, and adds it to this lock list. This method ensures the
   * parent is the expected parent inode, and the name of the inode is the expected name.
   *
   * NOTE: This method assumes that the inode path to the parent has been read locked.
   *
   * @param inode the inode to lock
   * @param parent the expected parent inode
   * @param name the expected name of the inode to be locked
   * @throws InvalidPathException if the inode is not consistent with the caller's expectations
   */
  public synchronized void lockReadAndCheckNameAndParent(INode inode, INode parent, byte[] name)
      throws InvalidPathException {
    mLocks.add(mInodeLockManager.lockReadAndCheckNameAndParent(inode, mUseTryLock, parent, name));
    mInodes.add(inode);
  }

  /**
   * Unlocks the last inode that was locked.
   */
  public synchronized void unlockLast() {
    if (mInodes.isEmpty()) {
      return;
    }
    mInodes.remove(mInodes.size() - 1);
    mLocks.removeLast().close();
  }

  /**
   * Locks the given inode in write mode, and adds it to this lock list. This call should only be
   * used when locking the root or an inode by id and not path or parent.
   *
   * @param inode the inode to lock
   */
  public synchronized void lockWrite(INode inode) {
    mLocks.add(mInodeLockManager.lockInode(inode, LockMode.WRITE, mUseTryLock));
    mInodes.add(inode);
  }

  /**
   * Locks the given inode in write mode, and adds it to this lock list. This method ensures the
   * parent is the expected parent inode.
   *
   * NOTE: This method assumes that the inode path to the parent has been read locked.
   *
   * @param inode the inode to lock
   * @param parent the expected parent inode
   * @throws InvalidPathException if the inode is not consistent with the caller's expectations
   */
  public synchronized void lockWriteAndCheckParent(INode inode, INode parent)
      throws InvalidPathException {
    mInodes.add(inode);
    mLocks.add(mInodeLockManager.lockWriteAndCheckParent(inode, mUseTryLock, parent));
  }

  /**
   * Locks the given inode in write mode, and adds it to this lock list. This method ensures the
   * parent is the expected parent inode, and the name of the inode is the expected name.
   *
   * NOTE: This method assumes that the inode path to the parent has been read locked.
   *
   * @param inode the inode to lock
   * @param parent the expected parent inode
   * @param name the expected name of the inode to be locked
   * @throws InvalidPathException if the inode is not consistent with the caller's expectations
   */
  public synchronized void lockWriteAndCheckNameAndParent(INode inode, INode parent, byte[] name)
      throws InvalidPathException {
    mLocks.add(mInodeLockManager.lockWriteAndCheckNameAndParent(
        inode, mUseTryLock, parent, name));
    mInodes.add(inode);
  }

  /**
   * @return a copy of the the list of inodes locked in this lock list, in order of when
   * the inodes were locked
   */
  // TODO(david): change this API to not return a copy
  public synchronized List<INode> getInodes() {
    return Lists.newArrayList(mInodes);
  }

  /**
   * @return true if the locklist is empty
   */
  public synchronized boolean isEmpty() {
    return mInodes.isEmpty();
  }

  @Override
  public synchronized void close() {
    for (int i = mLocks.size() - 1; i >= 0; i--) {
      mLocks.get(i).close();
    }
    mLocks.clear();
    mInodes.clear();
  }

  public InodeLockManager getInodeLockManager() {
    return mInodeLockManager;
  }

  public boolean isUseTryLock() {
    return mUseTryLock;
  }
}
