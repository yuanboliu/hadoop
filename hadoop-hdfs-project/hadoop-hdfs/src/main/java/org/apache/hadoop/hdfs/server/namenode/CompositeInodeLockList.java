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

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;

/**
 * Manages the locks for a list of {@link INode}s, based off an existing lock list. This does not
 * modify the base lock list, and when this lock list is closed, the base lock list is not closed.
 */
@ThreadSafe
public class CompositeInodeLockList extends InodeLockList {
  /** The base lock list for this composite list. */
  private final InodeLockList mBaseLockList;

  /**
   * Constructs a new lock list, using an existing lock list as the base list.
   *
   * @param baseLockList the base {@link InodeLockList} to use
   */
  public CompositeInodeLockList(InodeLockList baseLockList) {
    mBaseLockList = baseLockList;
  }

  /**
   * Constructs a new lock list, using an existing lock list as the base list.
   *
   * @param baseLockList the base {@link InodeLockList} to use
   * @param descendantLockList the locklist extension
   */
  public CompositeInodeLockList(InodeLockList baseLockList, InodeLockList descendantLockList) {
    mBaseLockList = baseLockList;
    mInodes = descendantLockList.mInodes;
    mLockModes = descendantLockList.mLockModes;
  }

  @Override
  public synchronized List<INode> getInodes() {
    // Combine the base list of inodes first.
    List<INode> ret = Lists.newArrayList(mBaseLockList.getInodes());
    ret.addAll(mInodes);
    return ret;
  }

  /**
   * @return true if the locklist is empty
   */
  @Override
  public synchronized boolean isEmpty() {
    return mBaseLockList.isEmpty() && mInodes.isEmpty();
  }
}
