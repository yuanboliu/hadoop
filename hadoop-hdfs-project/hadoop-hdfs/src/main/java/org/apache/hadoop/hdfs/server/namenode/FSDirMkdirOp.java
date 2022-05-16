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
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.CURRENT_STATE_ID;
import static org.apache.hadoop.util.Time.now;

class FSDirMkdirOp {

  static FileStatus mkdirs(FSNamesystem fsn, FSPermissionChecker pc, String src,
      PermissionStatus permissions, boolean createParent) throws IOException {
    FSDirectory fsd = fsn.getFSDirectory();
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.mkdirs: " + src);
    }
    try (INodesInPath iip =
        fsd.lockInodePath(src, FSDirectory.LockMode.WRITE)){
      final INode lastINode = iip.getLastINode();
      if (lastINode != null && lastINode.isFile()) {
        throw new FileAlreadyExistsException("Path is not a directory: " + src);
      }
      if (!iip.fullPathExists()) {
        if (fsd.isPermissionEnabled()) {
          fsd.checkAncestorAccess(pc, iip, FsAction.WRITE);
        }

        if (!createParent) {
          fsd.verifyParentDir(iip);
        }

        // validate that we have enough inodes. This is, at best, a
        // heuristic because the mkdirs() operation might need to
        // create multiple inodes.
        fsn.checkFsObjectLimit();

        // Ensure that the user can traversal the path by adding implicit
        // u+wx permission to all ancestor directories.
        INodesInPath existing =
            createParentDirectories(fsd, iip, permissions, false);
        if (existing != null) {
          existing = createSingleDirectory(
              fsd, existing, iip.getLastLocalName(), permissions);
        }
        if (existing == null) {
          throw new IOException("Failed to create directory: " + src);
        }
        return fsd.getAuditFileInfo(existing);
      }
      return fsd.getAuditFileInfo(iip);
    }
  }

  /**
   * For a given absolute path, create all ancestors as directories along the
   * path. All ancestors inherit their parent's permission plus an implicit
   * u+wx permission. This is used by create() and addSymlink() for
   * implicitly creating all directories along the path.
   *
   * For example, path="/foo/bar/spam", "/foo" is an existing directory,
   * "/foo/bar" is not existing yet, the function will create directory bar.
   *
   * @return a INodesInPath with all the existing and newly created
   *         ancestor directories created.
   *         Or return null if there are errors.
   */
  static INodesInPath createAncestorDirectories(
      FSDirectory fsd, INodesInPath iip, PermissionStatus permission)
      throws IOException {
    return createParentDirectories(fsd, iip, permission, true);
  }

  /**
   * Create all ancestor directories and return the parent inodes.
   *
   * @param fsd FSDirectory
   * @param existing inodes in path to the fs directory
   * @param perm the permission of the directory. Note that all ancestors
   *             created along the path has implicit {@code u+wx} permissions.
   * @param inheritPerms if the ancestor directories should inherit permissions
   *                 or use the specified permissions.
   *
   * @return {@link INodesInPath} which contains all inodes to the
   * target directory, After the execution parentPath points to the path of
   * the returned INodesInPath. The function return null if the operation has
   * failed.
   */
  private static INodesInPath createParentDirectories(FSDirectory fsd,
      INodesInPath existing, PermissionStatus perm, boolean inheritPerms)
      throws IOException {
    int pathLength = existing.getPathComponents().length;
    int existPathLength = existing.length();

    int missing = pathLength - existPathLength;
    if (missing == 0) {
      // full path exists, return existing. When createSingleDirectory is created later, the end
      // inode is created, but will not be addChild so the creation logic will not be affected.
      return existing;
    } else if (missing > 1) { // need to create at least one ancestor dir.
      // Ensure that the user can traversal the path by adding implicit
      // u+wx permission to all ancestor directories.
      PermissionStatus basePerm = inheritPerms
          ? existing.getLastINode().getPermissionStatus()
          : perm;
      perm = addImplicitUwx(basePerm, perm);
      // create all the missing directories.
      final int last = pathLength - 2;
      for (int i = existPathLength; existing != null && i <= last; i++) {
        byte[] component = existing.getPathComponent(i);
        existing = createSingleDirectory(fsd, existing, component, perm);
      }
    }
    return existing;
  }

  static void mkdirForEditLog(FSDirectory fsd, long inodeId, String src,
      PermissionStatus permissions, List<AclEntry> aclEntries, long timestamp)
      throws QuotaExceededException, AclException, FileAlreadyExistsException {
    try (INodesInPath existing =
        fsd.lockInodePath(src, FSDirectory.LockMode.WRITE)) {
      final byte[] localName = existing.getLocalNameByInodesSize();
      Preconditions.checkState(existing.getLastLockListInode() != null);
      unprotectedMkdir(fsd, inodeId, existing, localName, permissions, aclEntries,
              timestamp);
    }
  }

  private static INodesInPath createSingleDirectory(FSDirectory fsd,
      INodesInPath existing, byte[] localName, PermissionStatus perm)
      throws IOException {
    existing = unprotectedMkdir(fsd, fsd.allocateNewInodeId(), existing,
        localName, perm, null, now());
    if (existing == null) {
      return null;
    }

    final INode newNode = existing.getLastLockListInode();
    // Directory creation also count towards FilesCreated
    // to match count of FilesDeleted metric.
    NameNode.getNameNodeMetrics().incrFilesCreated();

    String cur = existing.getPath();
    fsd.getEditLog().logMkDir(cur, newNode);
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("mkdirs: created directory " + cur);
    }
    return existing;
  }

  private static PermissionStatus addImplicitUwx(PermissionStatus parentPerm,
      PermissionStatus perm) {
    FsPermission p = parentPerm.getPermission();
    FsPermission ancestorPerm = new FsPermission(
        p.getUserAction().or(FsAction.WRITE_EXECUTE),
        p.getGroupAction(),
        p.getOtherAction());
    return new PermissionStatus(perm.getUserName(), perm.getGroupName(),
        ancestorPerm);
  }

  /**
   * create a directory at path specified by parent
   */
  private static INodesInPath unprotectedMkdir(FSDirectory fsd, long inodeId,
      INodesInPath parent, byte[] name, PermissionStatus permission,
      List<AclEntry> aclEntries, long timestamp)
      throws QuotaExceededException, AclException, FileAlreadyExistsException {
    if (!parent.getLastLockListInode().isDirectory()) {
      throw new FileAlreadyExistsException("Parent path is not a directory: " +
          parent.getPath() + " " + DFSUtil.bytes2String(name));
    }
    INode lastExistingInode = parent.getLastLockListInode();

    final INodeDirectory dir = new INodeDirectory(inodeId, name, permission,
        timestamp);
    dir.setParent(parent.getLastLockListInode().asDirectory());
    // Lock the newly created inode before subsequent operations, and add it to the lock group.
    parent.mLockList.lockWriteAndCheckParent(dir, parent.getLastLockListInode());
    if (!lastExistingInode.asDirectory().addChild(dir)) {

      // The inode couldn't be added, so we will not add it to the tree and it should soon be
      // garbage collected. We mark it deleted as a precautionary measure in case something
      // manages to get a reference the inode.
      dir.setDeleted(true);
      parent.unlockLast();

      int i=0;
      while (true) {
        if (i > 1000) {
          throw new FileAlreadyExistsException("Directory "+ DFSUtil.bytes2String(name) +
              " already exists and cannot get locked child during 1000 attempts.");
        }
        i++;
        INode child = lastExistingInode.asDirectory().getChild(name, CURRENT_STATE_ID);
        parent.mLockList.lockWriteAndCheckNameAndParent(child, lastExistingInode, name);
        if (child != lastExistingInode.asDirectory().getChild(name, CURRENT_STATE_ID)) {
          // The locked child has changed, so unlock and try again.
          parent.unlockLast();
          continue;
        }
        break;
      }
    }

    if (parent != null && aclEntries != null) {
      AclStorage.updateINodeAcl(dir, aclEntries, CURRENT_STATE_ID);
    }
    return parent;
  }
}

