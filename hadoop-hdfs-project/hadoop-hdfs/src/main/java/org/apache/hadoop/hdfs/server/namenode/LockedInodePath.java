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

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.lock.exception.ExceptionMessage;
import org.apache.hadoop.hdfs.server.lock.util.io.PathUtils;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.util.List;

/**
 * This class represents a path of locked {@link INode}, starting from the root.
 */
@ThreadSafe
public abstract class LockedInodePath implements Closeable {
  protected final Path mUri;
  protected final String[] mPathComponents;
  protected final InodeLockList mLockList;
  protected FSDirectory.LockMode mLockMode;

  LockedInodePath(Path uri, InodeLockList lockList,
      FSDirectory.LockMode lockMode)
      throws InvalidPathException {
    Preconditions.checkArgument(!lockList.isEmpty());
    mUri = uri;
    mPathComponents = PathUtils.getPathComponents(mUri.toString());
    mLockList = lockList;
    mLockMode = lockMode;
  }

  LockedInodePath(Path uri, InodeLockList lockList, String[] pathComponents,
      FSDirectory.LockMode lockMode) {
    Preconditions.checkArgument(!lockList.isEmpty());
    mUri = uri;
    mPathComponents = pathComponents;
    mLockList = lockList;
    mLockMode = lockMode;
  }

  /**
   * Creates a new instance of {@link LockedInodePath}, that is the descendant of an existing
   * lockedInodePath.
   *
   * @param descendantUri the uri of the descendant
   * @param lockedInodePath the lockedInodePath that is the parent of the descendant
   * @param lockList the lockList which contains all the locks from the parent (not including)
   *                to the descendant.
   */
  LockedInodePath(Path descendantUri, LockedInodePath lockedInodePath,
                  InodeLockList lockList) throws InvalidPathException {
    mUri = descendantUri;
    mPathComponents = PathUtils.getPathComponents(mUri.toString());
    mLockList = new CompositeInodeLockList(lockedInodePath.mLockList, lockList);
    mLockMode = lockedInodePath.getLockMode();
  }

  /**
   * @return the full uri of the path
   */
  public synchronized Path getUri() {
    return mUri;
  }

  /**
   * @return the target inode
   * @throws FileNotFoundException if the target inode does not exist
   */
  public synchronized INode getInode() throws FileNotFoundException {
    INode inode = getInodeOrNull();
    if (inode == null) {
      throw new FileNotFoundException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(mUri));
    }
    return inode;
  }

  /**
   * @return the target inode, or null if it does not exist
   */
  @Nullable
  public synchronized INode getInodeOrNull() {
    if (!fullPathExists()) {
      return null;
    }
    List<INode> inodeList = mLockList.getInodes();
    return inodeList.get(inodeList.size() - 1);
  }

  /**
   * @return the target inode as an {@link INodeFile}
   * @throws FileNotFoundException if the target inode does not exist, or it is not a file
   */
  public synchronized  INodeFile getInodeFile() throws
      FileNotFoundException {
    INode inode = getInode();
    if (!inode.isFile()) {
      throw new FileNotFoundException(ExceptionMessage.PATH_MUST_BE_FILE.getMessage(mUri));
    }
    return (INodeFile) inode;
  }

  /**
   * @return the parent of the target inode
   * @throws InvalidPathException if the parent inode is not a directory
   * @throws FileNotFoundException if the parent of the target does not exist
   */
  public synchronized INodeDirectory getParentInodeDirectory()
      throws InvalidPathException, FileNotFoundException {
    INode inode = getParentInodeOrNull();
    if (inode == null) {
      throw new FileNotFoundException(
          ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(mUri.getParent()));
    }
    if (!inode.isDirectory()) {
      throw new InvalidPathException(
          ExceptionMessage.PATH_MUST_HAVE_VALID_PARENT.getMessage(mUri));
    }
    return (INodeDirectory) inode;
  }

  /**
   * @return the parent of the target inode, or null if the parent does not exist
   */
  @Nullable
  public synchronized INode getParentInodeOrNull() {
    if (mPathComponents.length < 2 || mLockList.getInodes().size() < (mPathComponents.length - 1)) {
      // The path is only the root, or the list of inodes is not long enough to contain the parent
      return null;
    }
    return mLockList.getInodes().get(mPathComponents.length - 2);
  }

  /**
   * @return the last existing inode on the inode path
   */
  public synchronized INode getLastExistingInode() {
    return mLockList.getInodes().get(mLockList.getInodes().size() - 1);
  }
  /**
   * @return a copy of the list of existing inodes, from the root
   */
  public synchronized List<INode> getInodeList() {
    return mLockList.getInodes();
  }

  /**
   * @return true if the entire path of inodes exists, false otherwise
   */
  public synchronized boolean fullPathExists() {
    return mLockList.getInodes().size() == mPathComponents.length;
  }

  /**
   * @return the {@link FSDirectory.LockMode} of this path
   */
  public synchronized FSDirectory.LockMode getLockMode() {
    return mLockMode;
  }

  @Override
  public synchronized void close() {
    mLockList.close();
  }

  /**
   * Downgrades the last inode that was locked, if the inode was previously WRITE locked. If the
   * inode was previously READ locked, no additional locking will occur.
   */
  public synchronized void downgradeLast() {
    mLockList.downgradeLast();
  }

  /**
   * Downgrades the last inode that was locked, according to the specified {@link LockingScheme}.
   * If the locking scheme initially desired the READ lock, the downgrade will occur. Otherwise,
   * downgrade will not be performed.
   *
   * @param lockingScheme the locking scheme to inspect
   */
  public synchronized void downgradeLastWithScheme(LockingScheme lockingScheme) {
    // Need to downgrade if the locking scheme initially desired the READ lock.
    if (lockingScheme.getMode() == FSDirectory.LockMode.READ) {
      downgradeLast();
      mLockMode = FSDirectory.LockMode.READ;
    }
  }

  /**
   * Returns the closest ancestor of the target inode (last inode in the full path).
   *
   * @return the closest ancestor inode
   * @throws FileNotFoundException if an ancestor does not exist
   */
  public synchronized INode getAncestorInode() throws FileNotFoundException {
    int ancestorIndex = mPathComponents.length - 2;
    if (ancestorIndex < 0) {
      throw new FileNotFoundException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(mUri));
    }
    ancestorIndex = Math.min(ancestorIndex, mLockList.getInodes().size() - 1);
    return mLockList.getInodes().get(ancestorIndex);
  }

  /**
   * Constructs a temporary {@link LockedInodePath} from an existing {@link LockedInodePath}, for
   * a direct child of the existing path. The child does not exist yet, this method simply adds
   * the child to the path.
   *
   * @param childName the name of the direct child
   * @return a {@link LockedInodePath} for the direct child
   * @throws InvalidPathException if the path is invalid
   */
  public synchronized LockedInodePath createTempPathForChild(String childName)
      throws InvalidPathException {
    Preconditions.checkNotNull(getInodeOrNull());
    Preconditions.checkState(getInodeOrNull().isDirectory(),
        "Trying to create TempPathForChild for a file inode");
    return new MutableLockedInodePath(new Path(mUri, childName), new CompositeInodeLockList(mLockList),
        mLockMode);
  }

  /**
   * Constructs a temporary {@link LockedInodePath} from an existing {@link LockedInodePath}, for
   * a direct child of the existing path. The child must exist and this method will lock the child.
   * A new {@link LockedInodePath} object is returned. When the returned temporary path is closed,
   * it does not close the existing path.
   *
   * @param child the inode of the direct child
   * @param lockMode the desired locking mode for the child
   * @return a {@link LockedInodePath} for the direct child
   * @throws InvalidPathException if the path is invalid
   * @throws FileNotFoundException if the file does not exist
   */
  public synchronized LockedInodePath createTempPathForExistingChild(
      INode child, FSDirectory.LockMode lockMode)
      throws InvalidPathException, FileNotFoundException {
    InodeLockList lockList = new CompositeInodeLockList(mLockList);
    LockedInodePath lockedDescendantPath;
    if (lockMode == FSDirectory.LockMode.READ) {
      lockList.lockReadAndCheckParent(child, getInode());
      lockedDescendantPath = new MutableLockedInodePath(
          new Path(getUri(), child.getLocalName()), this, lockList);
    } else {
      lockList.lockWriteAndCheckParent(child, getInode());
      lockedDescendantPath = new MutableLockedInodePath(
          new Path(getUri(), child.getLocalName()), this, lockList);
    }
    return lockedDescendantPath;
  }

  /**
   * Unlocks the last inode that was locked.
   */
  public synchronized void unlockLast() {
    if (mLockList.getInodes().isEmpty()) {
      return;
    }
    mLockList.unlockLast();
  }
}
