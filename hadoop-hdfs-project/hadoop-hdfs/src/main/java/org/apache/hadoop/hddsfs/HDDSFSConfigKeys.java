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

package org.apache.hadoop.hddsfs;

public interface HDDSFSConfigKeys {
  String HDDSFS_NAMENODE_LOCK_POOL_INITSIZE_KEY
      = "hddsfs.namenode.lock.pool.initsize";
  int HDDSFS_NAMENODE_LOCK_POOL_INITSIZE_KEY_DEFAULT = 1000;
  String HDDSFS_NAMENODE_LOCK_POOL_LOW_WATERMARK_KEY
      = "hddsfs.namenode.lock.pool.low.watermark";
  int HDDSFS_NAMENODE_LOCK_POOL_LOW_WATERMARK_DEFAULT = 500000;
  String HDDSFS_NAMENODE_LOCK_POOL_HIGH_WATERMARK_KEY
      = "hddsfs.namenode.lock.pool.high.watermark";
  int HDDSFS_NAMENODE_LOCK_POOL_HIGH_WATERMARK_DEFAULT = 1000000;
  String HDDSFS_NAMENODE_LOCK_POOL_CONCURRENCY_LEVEL_KEY
      = "hddsfs.namenode.lock.pool.concurrency.level";
  int HDDSFS_NAMENODE_LOCK_POOL_CONCURRENCY_LEVEL_DEFAULT = 100;
}
