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

package org.apache.hadoop.hdfs.server.lock.collections;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toSet;

/**
 * A two-level concurrent map implementation. Concurrent usage is managed by the outer map.
 *
 * Users can supply the inner map type of their choice.
 *
 * @param <K1> the first key type
 * @param <K2> the second key type
 * @param <V> the value type
 * @param <M> the type for the inner map
 */
public class TwoKeyConcurrentMap<K1, K2, V, M extends Map<K2, V>> extends ConcurrentHashMap<K1, M> {
  private static final long serialVersionUID = 1L;

  private final Supplier<M> mInnerMapFn;

  /**
   * @param innerMapCreator supplier for the inner map type
   */
  public TwoKeyConcurrentMap(Supplier<M> innerMapCreator) {
    mInnerMapFn = innerMapCreator;
  }

  /**
   * @param k1 the first key
   * @param k2 the second key
   * @param v the value
   */
  public void addInnerValue(K1 k1, K2 k2, V v) {
    compute(k1, (k, inner) -> {
      if (inner == null) {
        inner = mInnerMapFn.get();
      }
      inner.put(k2, v);
      return inner;
    });
  }

  /**
   * @param k1 the first key
   * @param k2 the second key
   */
  @Nullable
  public void removeInnerValue(K1 k1, K2 k2) {
    computeIfPresent(k1, (k, inner) -> {
      inner.remove(k2);
      if (inner.isEmpty()) {
        return null;
      }
      return inner;
    });
  }

  /**
   * Flattens the (key1, key2, value) triples according to the given function.
   *
   * @param fn a function to create an entry from the keys and value
   * @param <R> the result type of the function
   * @return the set of entries
   */
  public <R> Set<R> flattenEntries(TriFunction<K1, K2, V, R> fn) {
    return entrySet().stream()
        .flatMap(outer -> outer.getValue().entrySet().stream()
            .map(inner -> fn.apply(outer.getKey(), inner.getKey(), inner.getValue())))
        .collect(toSet());
  }

  /**
   * A function with three arguments.
   *
   * @param <A> the first argument
   * @param <B> the second argument
   * @param <C> the third argument
   * @param <R> the return type
   */
  @FunctionalInterface
  public interface TriFunction<A, B, C, R> {
    /**
     * @param a the first argument
     * @param b the second argument
     * @param c the third argument
     * @return the result
     */
    R apply(A a, B b, C c);
  }

  /**
   * The equals implementation for this map simply uses the superclass's equals.
   */
  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  /**
   * The hashCode implementation for this map simply uses the superclass's.
   */
  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
