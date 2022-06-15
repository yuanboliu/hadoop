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

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;

/**
 * This class represents a list of locked inodePaths.
 */
@ThreadSafe
public class LockedInodePathList implements AutoCloseable {
  private final List<INodesInPath> mInodePathList;

  /**
   * Creates a new instance of {@link LockedInodePathList}.
   *
   * @param inodePathList the list to be closed
   */
  public LockedInodePathList(List<INodesInPath> inodePathList) {
    mInodePathList = inodePathList;
  }

  /**
   * get the associated inodePathList.
   * @return the list of inodePaths
   */
  public List<INodesInPath> getInodePathList() {
    return mInodePathList;
  }

  @Override
  public void close() {
    for (INodesInPath lockedInodePath: mInodePathList) {
      lockedInodePath.close();
    }
  }
}
