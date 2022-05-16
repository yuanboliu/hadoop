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

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.lock.exception.ExceptionMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.CURRENT_STATE_ID;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.ID_INTEGER_COMPARATOR;

/**
 * Contains INodes information resolved from a given path.
 */
@ThreadSafe
public class INodesInPath implements Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(INodesInPath.class);

  /**
   * @return true if path component is {@link HdfsConstants#DOT_SNAPSHOT_DIR}
   */
  private static boolean isDotSnapshotDir(byte[] pathComponent) {
    return pathComponent != null &&
        Arrays.equals(HdfsServerConstants.DOT_SNAPSHOT_DIR_BYTES, pathComponent);
  }

  @Deprecated
  private static INode[] getINodes(final INode inode) {
    int depth = 0, index;
    INode tmp = inode;
    while (tmp != null) {
      depth++;
      tmp = tmp.getParent();
    }
    INode[] inodes = new INode[depth];
    tmp = inode;
    index = depth;
    while (tmp != null) {
      index--;
      inodes[index] = tmp;
      tmp = tmp.getParent();
    }
    return inodes;
  }

  private static byte[][] getPaths(final INode[] inodes) {
    byte[][] paths = new byte[inodes.length][];
    for (int i = 0; i < inodes.length; i++) {
      paths[i] = inodes[i].getKey();
    }
    return paths;
  }

  /**
   * Construct {@link INodesInPath} from {@link INode}.
   *
   * @param inode to construct from
   * @return INodesInPath
   * @deprecated use {@link FSDirectory#lockFullInodePath(long, FSDirectory.LockMode)} instead.
   */
  @Deprecated
  static INodesInPath fromINode(INode inode) {
    INode[] inodes = getINodes(inode);
    byte[][] paths = getPaths(inodes);
    return new INodesInPath(inodes, paths);
  }

  /**
   * Construct {@link INodesInPath} from {@link INode} and its root
   * {@link INodeDirectory}. INodesInPath constructed this way will
   * each have its snapshot and latest snapshot id filled in.
   *
   * This routine is specifically for
   * {@link LeaseManager#getINodeWithLeases(INodeDirectory)} to get
   * open files along with their snapshot details which is used during
   * new snapshot creation to capture their meta data.
   *
   * @param rootDir the root {@link INodeDirectory} under which inode
   *                needs to be resolved
   * @param inode the {@link INode} to be resolved
   * @return INodesInPath
   */
  @Deprecated
  static INodesInPath fromINode(final INodeDirectory rootDir, INode inode) {
    byte[][] paths = getPaths(getINodes(inode));
    return resolve(rootDir, paths);
  }

  /**
   * @param components
   * @return
   *
   * @deprecated Use
   * {@link FSDirectory#lockInodePath(byte[][], FSDirectory.LockMode)} instead.
   */
  @Deprecated
  static INodesInPath fromComponents(byte[][] components) {
    return new INodesInPath(new INode[components.length], components);
  }

  /**
   * Retrieve existing INodes from a path.  The number of INodes is equal
   * to the number of path components.  For a snapshot path
   * (e.g. /foo/.snapshot/s1/bar), the ".snapshot/s1" will be represented in
   * one path component corresponding to its Snapshot.Root inode.  This 1-1
   * mapping ensures the path can always be properly reconstructed.
   *
   * <p>
   * Example: <br>
   * Given the path /c1/c2/c3 where only /c1/c2 exists, resulting in the
   * following path components: ["","c1","c2","c3"]
   * 
   * <p>
   * <code>getExistingPathINodes(["","c1","c2"])</code> should fill
   * the array with [rootINode,c1,c2], <br>
   * <code>getExistingPathINodes(["","c1","c2","c3"])</code> should
   * fill the array with [rootINode,c1,c2,null]
   * 
   * @param startingDir the starting directory
   * @param components array of path component name
   * @return the specified number of existing INodes in the path
   */
  @Deprecated
  static INodesInPath resolve(final INodeDirectory startingDir,
      final byte[][] components) {
    return resolve(startingDir, components, false);
  }

  @Deprecated
  static INodesInPath resolve(final INodeDirectory startingDir,
      byte[][] components, final boolean isRaw) {
    Preconditions.checkArgument(startingDir.compareTo(components[0]) == 0);

    INode curNode = startingDir;
    int count = 0;
    int inodeNum = 0;
    INode[] inodes = new INode[components.length];
    boolean isSnapshot = false;
    int snapshotId = CURRENT_STATE_ID;

    while (count < components.length && curNode != null) {
      final boolean lastComp = (count == components.length - 1);
      inodes[inodeNum++] = curNode;
      final boolean isRef = curNode.isReference();
      final boolean isDir = curNode.isDirectory();
      final INodeDirectory dir = isDir? curNode.asDirectory(): null;
      if (!isRef && isDir && dir.isWithSnapshot()) {
        //if the path is a non-snapshot path, update the latest snapshot.
        if (!isSnapshot && shouldUpdateLatestId(
            dir.getDirectoryWithSnapshotFeature().getLastSnapshotId(),
            snapshotId)) {
          snapshotId = dir.getDirectoryWithSnapshotFeature().getLastSnapshotId();
        }
      } else if (isRef && isDir && !lastComp) {
        // If the curNode is a reference node, need to check its dstSnapshot:
        // 1. if the existing snapshot is no later than the dstSnapshot (which
        // is the latest snapshot in dst before the rename), the changes 
        // should be recorded in previous snapshots (belonging to src).
        // 2. however, if the ref node is already the last component, we still 
        // need to know the latest snapshot among the ref node's ancestors, 
        // in case of processing a deletion operation. Thus we do not overwrite
        // the latest snapshot if lastComp is true. In case of the operation is
        // a modification operation, we do a similar check in corresponding 
        // recordModification method.
        if (!isSnapshot) {
          int dstSnapshotId = curNode.asReference().getDstSnapshotId();
          if (snapshotId == CURRENT_STATE_ID || // no snapshot in dst tree of rename
              (dstSnapshotId != CURRENT_STATE_ID &&
               dstSnapshotId >= snapshotId)) { // the above scenario
            int lastSnapshot = CURRENT_STATE_ID;
            DirectoryWithSnapshotFeature sf;
            if (curNode.isDirectory() && 
                (sf = curNode.asDirectory().getDirectoryWithSnapshotFeature()) != null) {
              lastSnapshot = sf.getLastSnapshotId();
            }
            snapshotId = lastSnapshot;
          }
        }
      }
      if (lastComp || !isDir) {
        break;
      }

      final byte[] childName = components[++count];
      // check if the next byte[] in components is for ".snapshot"
      if (isDotSnapshotDir(childName) && dir.isSnapshottable()) {
        isSnapshot = true;
        // check if ".snapshot" is the last element of components
        if (count == components.length - 1) {
          break;
        }
        // Resolve snapshot root
        final Snapshot s = dir.getSnapshot(components[count + 1]);
        if (s == null) {
          curNode = null; // snapshot not found
        } else {
          curNode = s.getRoot();
          snapshotId = s.getId();
        }
        // combine .snapshot & name into 1 component element to ensure
        // 1-to-1 correspondence between components and inodes arrays is
        // preserved so a path can be reconstructed.
        byte[][] componentsCopy =
            Arrays.copyOf(components, components.length - 1);
        componentsCopy[count] = DFSUtil.string2Bytes(
            DFSUtil.byteArray2PathString(components, count, 2));
        // shift the remaining components after snapshot name
        int start = count + 2;
        System.arraycopy(components, start, componentsCopy, count + 1,
            components.length - start);
        components = componentsCopy;
        // reduce the inodes array to compensate for reduction in components
        inodes = Arrays.copyOf(inodes, components.length);
      } else {
        // normal case, and also for resolving file/dir under snapshot root
        curNode = dir.getChild(childName,
            isSnapshot ? snapshotId : CURRENT_STATE_ID);
      }
    }
    return new INodesInPath(inodes, components, isRaw, isSnapshot, snapshotId);
  }

  private static boolean shouldUpdateLatestId(int sid, int snapshotId) {
    return snapshotId == CURRENT_STATE_ID || (sid != CURRENT_STATE_ID &&
        ID_INTEGER_COMPARATOR.compare(snapshotId, sid) < 0);
  }

  /**
   * Replace an inode of the given INodesInPath in the given position. We do a
   * deep copy of the INode array.
   * @param pos the position of the replacement
   * @param inode the new inode
   * @return a new INodesInPath instance
   *
   * @Deprecated use {@link #unlockLast()} instead, when remove last.
   * use {@link InodeLockList#lockWrite(INode) instead.
   */
  @Deprecated
  public static INodesInPath replace(INodesInPath iip, int pos, INode inode) {
    Preconditions.checkArgument(iip.length() > 0 && pos > 0 // no for root
        && pos < iip.length());
    if (iip.getINode(pos) == null) {
      Preconditions.checkState(iip.getINode(pos - 1) != null);
    }
    INode[] inodes = new INode[iip.inodes.length];
    System.arraycopy(iip.inodes, 0, inodes, 0, inodes.length);
    inodes[pos] = inode;
    return new INodesInPath(inodes, iip.path, iip.isRaw,
        iip.isSnapshot, iip.snapshotId);
  }

  /**
   * Extend a given INodesInPath with a child INode. The child INode will be
   * appended to the end of the new INodesInPath.
   *
   * @Deprecated use {@link InodeLockList#lockWrite(INode) instead.
   */
  @Deprecated
  public static INodesInPath append(INodesInPath iip, INode child,
      byte[] childName) {
    Preconditions.checkArgument(iip.length() > 0);
    Preconditions.checkArgument(iip.getLastINode() != null && iip
        .getLastINode().isDirectory());
    INode[] inodes = new INode[iip.length() + 1];
    System.arraycopy(iip.inodes, 0, inodes, 0, inodes.length - 1);
    inodes[inodes.length - 1] = child;
    byte[][] path = new byte[iip.path.length + 1][];
    System.arraycopy(iip.path, 0, path, 0, path.length - 1);
    path[path.length - 1] = childName;
    return new INodesInPath(inodes, path, iip.isRaw,
        iip.isSnapshot, iip.snapshotId);
  }

  private final byte[][] path;
  private volatile String pathname;

  /**
   * Array with the specified number of INodes resolved for a given path.
   */
  @Deprecated
  private final INode[] inodes;
  /**
   * true if this path corresponds to a snapshot
   */
  private final boolean isSnapshot;

  /**
   * true if this is a /.reserved/raw path.  path component resolution strips
   * it from the path so need to track it separately.
   */
  private final boolean isRaw;

  /**
   * For snapshot paths, it is the id of the snapshot; or 
   * {@link Snapshot#CURRENT_STATE_ID} if the snapshot does not exist. For 
   * non-snapshot paths, it is the id of the latest snapshot found in the path;
   * or {@link Snapshot#CURRENT_STATE_ID} if no snapshot is found.
   */
  private final int snapshotId;

  private final InodeLockList mLockList;
  protected FSDirectory.LockMode mLockMode;

  @Deprecated
  private INodesInPath(INode[] inodes, byte[][] path, boolean isRaw,
      boolean isSnapshot,int snapshotId) {
    Preconditions.checkArgument(inodes != null && path != null);
    this.inodes = inodes;
    this.path = path;
    this.isRaw = isRaw;
    this.isSnapshot = isSnapshot;
    this.snapshotId = snapshotId;

    mLockList = null;
  }

  @Deprecated
  private INodesInPath(INode[] inodes, byte[][] path) {
    this(inodes, path, false, false, CURRENT_STATE_ID);
  }

  INodesInPath(InodeLockList lockList, byte[][] pathComponents,
      FSDirectory.LockMode lockMode) {
    Preconditions.checkArgument(!lockList.isEmpty());
    path = pathComponents;
    mLockList = lockList;
    mLockMode = lockMode;

    // TODO(baoloongmao): fix snapshot and raw future.
    this.isSnapshot = false;
    this.snapshotId = CURRENT_STATE_ID;
    this.isRaw = false;
    inodes = null;
  }

  INodesInPath(String uri, InodeLockList lockList,
      FSDirectory.LockMode lockMode)
      throws InvalidPathException {
    this(lockList, INode.getPathComponents(uri), lockMode);
  }

  /**
   * Creates a new instance of {@link INodesInPath}, that is the descendant of an existing
   * lockedInodePath.
   *
   * @param descendantUri the uri of the descendant
   * @param lockedInodePath the lockedInodePath that is the parent of the descendant
   * @param lockList the lockList which contains all the locks from the parent (not including)
   *                to the descendant.
   */
  INodesInPath(String descendantUri, INodesInPath lockedInodePath,
      InodeLockList lockList) throws InvalidPathException {
    path = INode.getPathComponents(descendantUri);
    mLockList = new CompositeInodeLockList(lockedInodePath.mLockList, lockList);
    mLockMode = lockedInodePath.getLockMode();

    // TODO(baoloongmao): fix snapshot and raw future.
    this.isSnapshot = false;
    this.snapshotId = CURRENT_STATE_ID;
    this.isRaw = false;
    inodes = null;
  }

  /**
   * For non-snapshot paths, return the latest snapshot id found in the path.
   */
  public int getLatestSnapshotId() {
    Preconditions.checkState(!isSnapshot);
    return snapshotId;
  }
  
  /**
   * For snapshot paths, return the id of the snapshot specified in the path.
   * For non-snapshot paths, return {@link Snapshot#CURRENT_STATE_ID}.
   */
  public int getPathSnapshotId() {
    return isSnapshot ? snapshotId : CURRENT_STATE_ID;
  }


  /**
   * @return the i-th inode if i >= 0;
   *         otherwise, i < 0, return the (length + i)-th inode.
   */
  public INode getINode(int i) {
    if (inodes == null) {
      return mLockList.mInodes.get((i < 0) ? mLockList.mInodes.size() + i : i);
    }
    return inodes[(i < 0) ? inodes.length + i : i];
  }

  /**
   * @return the last inode.
   **/
  public INode getLastINode() {
    if (inodes == null) {
      return getLastInodeOrNull();
    }
    return getINode(-1);
  }

  byte[] getLastLocalName() {
    return path[path.length - 1];
  }

  byte[] getLocalNameByInodesSize() {
    return path[mLockList.mInodes.size()];
  }

  public byte[][] getPathComponents() {
    return path;
  }

  public byte[] getPathComponent(int i) {
    return path[i];
  }

  /** @return the full path in string form */
  public synchronized String getPath() {
    if (pathname == null) {
      pathname = DFSUtil.byteArray2PathString(path);
    }
    return pathname;
  }

  public String getParentPath() {
    return getPath(path.length - 2);
  }

  public String getPath(int pos) {
    return DFSUtil.byteArray2PathString(path, 0, pos + 1); // it's a length...
  }

  public int length() {
    if (inodes == null) {
      if (mLockList != null && mLockList.mInodes != null) {
        return mLockList.mInodes.size();
      }
      return -1;
    }
    return inodes.length;
  }

  public synchronized INode[] getINodesArray() {
    INode[] retArr = new INode[mLockList.mInodes.size()];
    retArr = mLockList.mInodes.toArray(retArr);
    return retArr;
  }

  /**
   * @param length number of ancestral INodes in the returned INodesInPath
   *               instance
   * @return the INodesInPath instance containing ancestral INodes. Note that
   * this method only handles non-snapshot paths.
   */
  @Deprecated
  private INodesInPath getAncestorINodesInPath(int length) {
    Preconditions.checkArgument(length >= 0 && length < inodes.length);
    Preconditions.checkState(isDotSnapshotDir() || !isSnapshot());
    final INode[] anodes = new INode[length];
    final byte[][] apath = new byte[length][];
    System.arraycopy(this.inodes, 0, anodes, 0, length);
    System.arraycopy(this.path, 0, apath, 0, length);
    return new INodesInPath(anodes, apath, isRaw, false, snapshotId);
  }

  /**
   * @return an INodesInPath instance containing all the INodes in the parent
   *         path. We do a deep copy here.
   */
  @Deprecated
  public INodesInPath getParentINodesInPath() {
    return inodes.length > 1 ? getAncestorINodesInPath(inodes.length - 1) :
        null;
  }

  /**
   * Verify if this {@link INodesInPath} is a descendant of the
   * requested {@link INodeDirectory}.
   *
   * @param inodeDirectory the ancestor directory
   * @return true if this INodesInPath is a descendant of inodeDirectory
   */
  @Deprecated
  public boolean isDescendant(final INodeDirectory inodeDirectory) {
    final INodesInPath dirIIP = fromINode(inodeDirectory);
    return isDescendant(dirIIP);
  }

  @Deprecated
  private boolean isDescendant(final INodesInPath ancestorDirIIP) {
    int ancestorDirINodesLength = ancestorDirIIP.length();
    int myParentINodesLength = length() - 1;
    if (myParentINodesLength < ancestorDirINodesLength) {
      return false;
    }

    int index = 0;
    while (index < ancestorDirINodesLength) {
      if (inodes[index] != ancestorDirIIP.getINode(index)) {
        return false;
      }
      index++;
    }
    return true;
  }


  /**
   * @return a new INodesInPath instance that only contains existing INodes.
   * Note that this method only handles non-snapshot paths.
   */
  @Deprecated
  public INodesInPath getExistingINodes() {
    Preconditions.checkState(!isSnapshot());
    for (int i = inodes.length; i > 0; i--) {
      if (inodes[i - 1] != null) {
        return (i == inodes.length) ? this : getAncestorINodesInPath(i);
      }
    }
    return null;
  }

  /**
   * @return isSnapshot true for a snapshot path
   */
  boolean isSnapshot() {
    return this.isSnapshot;
  }

  /**
   * @return if .snapshot is the last path component.
   */
  boolean isDotSnapshotDir() {
    return isDotSnapshotDir(getLastLocalName());
  }

  /**
   * @return if this is a /.reserved/raw path.
   */
  public boolean isRaw() {
    return isRaw;
  }

  private static String toString(INode inode) {
    return inode == null? null: inode.getLocalName();
  }

  @Override
  public String toString() {
    return toString(true);
  }

  private String toString(boolean vaildateObject) {
    if (vaildateObject) {
      validate();
    }

    final StringBuilder b = new StringBuilder(getClass().getSimpleName())
        .append(": path = ").append(getPath())
        .append("\n  inodes = ");
    if (mLockList == null || mLockList.mInodes == null) {
      b.append("null");
    } else if (mLockList.mInodes.size() == 0) {
      b.append("[]");
    } else {
      b.append("[").append(toString(mLockList.mInodes.get(0)));
      for(int i = 1; i < mLockList.mInodes.size(); i++) {
        b.append(", ").append(toString(mLockList.mInodes.get(i)));
      }
      b.append("], length=").append(mLockList.mInodes.size());
    }
    b.append("\n  isSnapshot        = ").append(isSnapshot)
     .append("\n  snapshotId        = ").append(snapshotId);
    return b.toString();
  }

  @Deprecated
  void validate() {
    // check parent up to snapshotRootIndex if this is a snapshot path
    if (inodes == null) {
      // skip validate avoid NPE.
      return;
    }

    int i = 0;
    if (inodes[i] != null) {
      for(i++; i < inodes.length && inodes[i] != null; i++) {
        final INodeDirectory parent_i = inodes[i].getParent();
        final INodeDirectory parent_i_1 = inodes[i-1].getParent();
        if (parent_i != inodes[i-1] &&
            (parent_i_1 == null || !parent_i_1.isSnapshottable()
                || parent_i != parent_i_1)) {
          throw new AssertionError(
              "inodes[" + i + "].getParent() != inodes[" + (i-1)
              + "]\n  inodes[" + i + "]=" + inodes[i].toDetailString()
              + "\n  inodes[" + (i-1) + "]=" + inodes[i-1].toDetailString()
              + "\n this=" + toString(false));
        }
      }
    }
    if (i != inodes.length) {
      throw new AssertionError("i = " + i + " != " + inodes.length
          + ", this=" + toString(false));
    }
  }


  /**
   * @return the target inode
   * @throws FileNotFoundException if the target inode does not exist
   */
  public synchronized INode getInode() throws FileNotFoundException {
    INode inode = getInodeOrNull();
    if (inode == null) {
      throw new FileNotFoundException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(getPath()));
    }
    return inode;
  }

  /**
   * @return the target inode
   */
  public synchronized INode getLastLockListInode() {
    List<INode> inodeList = mLockList.getInodes();
    return inodeList.get(length() - 1);
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
      throw new FileNotFoundException(ExceptionMessage.PATH_MUST_BE_FILE.getMessage(getPath()));
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
          ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(new Path(getPath()).getParent()));
    }
    if (!inode.isDirectory()) {
      throw new InvalidPathException(
          ExceptionMessage.PATH_MUST_HAVE_VALID_PARENT.getMessage(getPath()));
    }
    return (INodeDirectory) inode;
  }

  /**
   * @return the parent of the target inode, or null if the parent does not exist
   */
  @Nullable
  public synchronized INode getParentInodeOrNull() {
    if (path.length < 2 || mLockList.getInodes().size() < (path.length - 1)) {
      // The path is only the root, or the list of inodes is not long enough to contain the parent
      return null;
    }
    return mLockList.getInodes().get(path.length - 2);
  }

  /**
   * @return the last existing inode on the inode path
   */
  @Nullable
  public synchronized INode getLastInodeOrNull() {
    if (path.length > mLockList.getInodes().size()) {
      return null;
    }
    return getLastExistingInode();
  }

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
    return mLockList.getInodes().size() == path.length;
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
    int ancestorIndex = path.length - 2;
    if (ancestorIndex < 0) {
      throw new FileNotFoundException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(getPath()));
    }
    ancestorIndex = Math.min(ancestorIndex, mLockList.getInodes().size() - 1);
    return mLockList.getInodes().get(ancestorIndex);
  }

  /**
   * Constructs a temporary {@link INodesInPath} from an existing {@link INodesInPath}, for
   * a direct child of the existing path. The child does not exist yet, this method simply adds
   * the child to the path.
   *
   * @param childName the name of the direct child
   * @return a {@link INodesInPath} for the direct child
   * @throws InvalidPathException if the path is invalid
   */
  public synchronized INodesInPath createTempPathForChild(String childName)
      throws InvalidPathException {
    Preconditions.checkNotNull(getInodeOrNull());
    Preconditions.checkState(getInodeOrNull().isDirectory(),
        "Trying to create TempPathForChild for a file inode");
    return new MutableLockedInodePath(new Path(getPath(), childName).toString(),
        new CompositeInodeLockList(mLockList),
        mLockMode);
  }

  /**
   * Constructs a temporary {@link INodesInPath} from an existing {@link INodesInPath}, for
   * a direct child of the existing path. The child must exist and this method will lock the child.
   * A new {@link INodesInPath} object is returned. When the returned temporary path is closed,
   * it does not close the existing path.
   *
   * @param child the inode of the direct child
   * @param lockMode the desired locking mode for the child
   * @return a {@link INodesInPath} for the direct child
   * @throws InvalidPathException if the path is invalid
   * @throws FileNotFoundException if the file does not exist
   */
  public synchronized INodesInPath createTempPathForExistingChild(
      INode child, FSDirectory.LockMode lockMode)
      throws InvalidPathException, FileNotFoundException {
    InodeLockList lockList = new CompositeInodeLockList(mLockList);
    INodesInPath lockedDescendantPath;
    if (lockMode == FSDirectory.LockMode.READ) {
      lockList.lockReadAndCheckParent(child, getInode());
      lockedDescendantPath = new MutableLockedInodePath(
          new Path(getPath(), child.getLocalName()).toString(), this, lockList);
    } else {
      lockList.lockWriteAndCheckParent(child, getInode());
      lockedDescendantPath = new MutableLockedInodePath(
          new Path(getPath(), child.getLocalName()).toString(), this, lockList);
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

  protected synchronized InodeLockList getLockList() {
    return mLockList;
  }
}
