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

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.List;

/**
 * Manages the locks for a list of {@link INode}.
 */
@ThreadSafe
public class InodeLockList implements AutoCloseable {
  protected List<INode> mInodes;
  protected List<FSDirectory.LockMode> mLockModes;

  /**
   * Creates a new instance of {@link InodeLockList}.
   */
  public InodeLockList() {
    mInodes = new ArrayList<>();
    mLockModes = new ArrayList<>();
  }

  /**
   * Locks the given inode in read mode, and adds it to this lock list. This call should only be
   * used when locking the root or an inode by id and not path or parent.
   *
   * @param inode the inode to lock
   */
  public synchronized void lockRead(INode inode) {
    inode.lockRead();
    mInodes.add(inode);
    mLockModes.add(FSDirectory.LockMode.READ);
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
    inode.lockReadAndCheckParent(parent);
    mInodes.add(inode);
    mLockModes.add(FSDirectory.LockMode.READ);
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
  public synchronized void lockReadAndCheckNameAndParent(INode inode, INode parent, String name)
      throws InvalidPathException {
    inode.lockReadAndCheckNameAndParent(parent, name);
    mInodes.add(inode);
    mLockModes.add(FSDirectory.LockMode.READ);
  }

  /**
   * Unlocks the last inode that was locked.
   */
  public synchronized void unlockLast() {
    if (mInodes.isEmpty()) {
      return;
    }
    INode inode = mInodes.remove(mInodes.size() - 1);
    FSDirectory.LockMode lockMode = mLockModes.remove(mLockModes.size() - 1);
    if (lockMode == FSDirectory.LockMode.READ) {
      inode.unlockRead();
    } else {
      inode.unlockWrite();
    }
  }

  /**
   * Downgrades the last inode that was locked, if the inode was previously WRITE locked. If the
   * inode was previously READ locked, no additional locking will occur.
   */
  public synchronized void downgradeLast() {
    if (mInodes.isEmpty()) {
      return;
    }
    if (mLockModes.get(mLockModes.size() - 1) != FSDirectory.LockMode.READ) {
      // The last inode was previously WRITE locked, so downgrade the lock.
      INode inode = mInodes.get(mInodes.size() - 1);
      inode.lockRead();
      inode.unlockWrite();
      // Update the last lock mode to READ
      mLockModes.remove(mLockModes.size() - 1);
      mLockModes.add(FSDirectory.LockMode.READ);
    }
  }

  /**
   * Locks the given inode in write mode, and adds it to this lock list. This call should only be
   * used when locking the root or an inode by id and not path or parent.
   *
   * @param inode the inode to lock
   */
  public synchronized void lockWrite(INode inode) {
    inode.lockWrite();
    mInodes.add(inode);
    mLockModes.add(FSDirectory.LockMode.WRITE);
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
    inode.lockWriteAndCheckParent(parent);
    mInodes.add(inode);
    mLockModes.add(FSDirectory.LockMode.WRITE);
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
  public synchronized void lockWriteAndCheckNameAndParent(INode inode, INode parent, String name)
      throws InvalidPathException {
    inode.lockWriteAndCheckNameAndParent(parent, name);
    mInodes.add(inode);
    mLockModes.add(FSDirectory.LockMode.WRITE);
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
    for (int i = mInodes.size() - 1; i >= 0; i--) {
      INode inode = mInodes.get(i);
      FSDirectory.LockMode lockMode = mLockModes.get(i);
      if (lockMode == FSDirectory.LockMode.READ) {
        inode.unlockRead();
      } else {
        inode.unlockWrite();
      }
    }
    mInodes.clear();
    mLockModes.clear();
  }
}
