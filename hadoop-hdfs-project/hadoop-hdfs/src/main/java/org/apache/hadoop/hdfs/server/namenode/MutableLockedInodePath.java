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

import org.apache.hadoop.fs.InvalidPathException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents an {@link INodesInPath}, where the list of inodes can be extended to
 * gather additional inodes along the path.
 */
@ThreadSafe
public class MutableLockedInodePath extends INodesInPath {
  /**
   * Creates an instance of {@link MutableLockedInodePath}.
   *
   * @param uri the URI
   * @param lockList the lock list of the inodes
   * @param lockMode the lock mode for the path
   * @throws InvalidPathException if the path passed is invalid
   */
  // TODO(gpang): restructure class hierarchy, rename class
  public MutableLockedInodePath(String uri, InodeLockList lockList,
      FSDirectory.LockMode lockMode)
      throws InvalidPathException {
    super(uri, lockList, lockMode);
  }

  /**
   * Creates an instance of {@link MutableLockedInodePath}.
   *
   * @param lockList the lock list of the inodes
   * @param pathComponents the array of path components
   * @param lockMode the lock mode for the path
   */
  public MutableLockedInodePath(InodeLockList lockList,
      byte[][] pathComponents, FSDirectory.LockMode lockMode) {
    super(lockList, pathComponents, lockMode);
  }

  /**
   * Creates an instance of {@link MutableLockedInodePath}.
   *
   * @param descendantUri the URI
   * @param lockedInodePath lockedInodePath that is the parent
   * @param descendants Locked descendants
   * @throws InvalidPathException if the path passed is invalid
   */
  public MutableLockedInodePath(String descendantUri, INodesInPath lockedInodePath,
                                InodeLockList descendants) throws InvalidPathException {
    super(descendantUri, lockedInodePath, descendants);
  }
}
