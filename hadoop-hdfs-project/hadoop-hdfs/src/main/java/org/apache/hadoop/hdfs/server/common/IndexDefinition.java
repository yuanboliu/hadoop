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

/**
 * A class representing the definition of an index. Each instance of
 * this class must implement the method to define how to get the value of the field chosen as
 * the index key.
 *
 * @param <T> type of objects in this index
 */
@ThreadSafe
public abstract class IndexDefinition<T> {
  /** Whether it is a unique index. */
  //TODO(lei): change the mIsUnique to mIndexType enum
  private final boolean mIsUnique;

  /**
   * Constructs a new {@link IndexDefinition} instance.
   *
   * @param isUnique whether the index is unique. A unique index is an index where each index value
   *                 only maps to one object; A non-unique index is an index where an index value
   *                 can map to one or more objects.
   */
  public IndexDefinition(boolean isUnique) {
    mIsUnique = isUnique;
  }

  /**
   * @return whether the index requires all field values to be unique
   */
  public boolean isUnique() {
    return mIsUnique;
  }

  /**
   * Gets the value of the field that serves as index.
   *
   * @param o the instance to get the field value from
   * @return the field value
   */
  public abstract Object getFieldValue(T o);
}
