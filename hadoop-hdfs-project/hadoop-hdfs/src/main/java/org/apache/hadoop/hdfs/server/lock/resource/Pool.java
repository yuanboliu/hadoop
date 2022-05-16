/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdfs.server.lock.resource;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Interface representing a pool of resources to be temporarily used and returned.
 *
 * @param <T> the type of resource this pool manages
 */
public interface Pool<T> extends Closeable {
  /**
   * Acquires a resource from the pool.
   *
   * @return the acquired resource which should not be null
   */
  T acquire() throws IOException;

  /**
   * Acquires a resource from the pool.
   *
   * @param time time it takes before timeout if no resource is available
   * @param unit the unit of the time
   * @return the acquired resource which should not be null
   */
  T acquire(long time, TimeUnit unit) throws TimeoutException, IOException;

  /**
   * Releases the resource to the pool.
   *
   * @param resource the resource to release
   */
  void release(T resource);

  /**
   * @return the current pool size
   */
  int size();
}
