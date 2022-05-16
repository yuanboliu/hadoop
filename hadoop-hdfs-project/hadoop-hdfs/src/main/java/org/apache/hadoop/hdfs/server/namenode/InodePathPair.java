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

import org.apache.commons.math3.util.Pair;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents a pair of {@link INodesInPath}s. This is threadsafe, since the
 * elements cannot set once the pair is constructed.
 */
@ThreadSafe
public final class InodePathPair extends Pair<INodesInPath, INodesInPath>
    implements AutoCloseable {

  InodePathPair(INodesInPath inodePath1, INodesInPath inodePath2) {
    super(inodePath1, inodePath2);
  }

  @Override
  public synchronized void close() {
    getFirst().close();
    getSecond().close();
  }
}
