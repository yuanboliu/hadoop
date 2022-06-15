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

/**
 * A {@code CloseableResource<T>} is a wrapper around a resource of type {@code T} which must do
 * some sort of cleanup when it is no longer in use.
 *
 * @param <T> the type of the wrapped resource
 */
public abstract class CloseableResource<T> implements Closeable {
  private T mResource;

  /**
   * Creates a {@link CloseableResource} wrapper around the given resource. This resource will
   * be returned by the {@link CloseableResource#get()} method.
   *
   * @param resource the resource to wrap
   */
  public CloseableResource(T resource) {
    mResource = resource;
  }

  /**
   * @return the resource
   */
  public T get() {
    return mResource;
  }

  /**
   * Performs any cleanup operations necessary when the resource is no longer in use.
   */
  @Override
  public abstract void close();
}
