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
package org.apache.hadoop.hdfs.server.common;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A class representing a unique index. A unique index is an index
 * where each index value only maps to one object.
 *
 * @param <T> type of objects in this index
 */
@ThreadSafe
public class UniqueFieldIndex<T> implements FieldIndex<T> {
  private final IndexDefinition<T> mIndexDefinition;
  private final ConcurrentHashMap<Object, T> mIndexMap;

  /**
   * Constructs a new {@link UniqueFieldIndex} instance.
   *
   * @param indexDefinition definition of index
   */
  public UniqueFieldIndex(IndexDefinition<T> indexDefinition) {
    mIndexMap = new ConcurrentHashMap<>(8, 0.95f, 8);
    mIndexDefinition = indexDefinition;
  }

  @Override
  public boolean add(T object) {
    Object fieldValue = mIndexDefinition.getFieldValue(object);
    T previousObject = mIndexMap.putIfAbsent(fieldValue, object);

    if (previousObject != null && previousObject != object) {
      return false;
    }
    return true;
  }

  @Override
  public boolean remove(T object) {
    Object fieldValue = mIndexDefinition.getFieldValue(object);
    return mIndexMap.remove(fieldValue, object);
  }

  @Override
  public void clear() {
    mIndexMap.clear();
  }

  @Override
  public boolean containsField(Object fieldValue) {
    return mIndexMap.containsKey(fieldValue);
  }

  @Override
  public boolean containsObject(T object) {
    Object fieldValue = mIndexDefinition.getFieldValue(object);
    T res = mIndexMap.get(fieldValue);
    if (res == null) {
      return false;
    }
    return res == object;
  }

  @Override
  public Set<T> getByField(Object value) {
    T res = mIndexMap.get(value);
    if (res != null) {
      return Collections.singleton(res);
    }
    return Collections.emptySet();
  }

  @Override
  public T getFirst(Object value) {
    return mIndexMap.get(value);
  }

  @Override
  public Iterator<T> iterator() {
    return mIndexMap.values().iterator();
  }

  @Override
  public int size() {
    return mIndexMap.size();
  }
}
