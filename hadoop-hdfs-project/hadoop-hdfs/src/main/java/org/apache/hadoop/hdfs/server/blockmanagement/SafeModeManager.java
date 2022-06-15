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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportReplica;

/**
 * Block manager safe mode manager interface.
 */
@InterfaceStability.Evolving
public interface SafeModeManager {

  /**
   * Initialize the safe mode information.
   *
   * @param total initial total blocks
   */
  void activate(long total);


  /**
   * Stop and clean up safe mode manager.
   *
   */
  void close();

  /**
   * @return true if it stays in start up safe mode else false.
   */
  boolean isInSafeMode();

  /**
   * The transition of the safe mode state machine.
   * If safe mode is not currently on, this is a no-op.
   */
  void checkSafeMode();

  /**
   * Get insight of safe mode process.
   */
  String getSafeModeTip();

  /**
   * Leave start up safe mode.
   *
   * @param force - true to force exit
   * @return true if it leaves safe mode successfully else false
   */
  boolean leaveSafeMode(boolean force);

  /**
   * Adjust the total number of blocks safe and expected during safe mode.
   * If safe mode is not currently on, this is a no-op.
   *
   * @param deltaSafe  the change in number of safe blocks
   * @param deltaTotal the change in number of total blocks expected
   */
  void adjustBlockTotals(int deltaSafe, int deltaTotal);

  /**
   * Set total number of blocks.
   */
  void setBlockTotal(long total);

  /**
   * Increment number of safe blocks if the current block is contiguous
   * and it has reached minimal replication or
   * if the current block is striped and the number of its actual data blocks
   * reaches the number of data units specified by the erasure coding policy.
   * If safe mode is not currently on, this is a no-op.
   *
   * @param storageNum  current number of replicas or number of internal blocks
   *                    of a striped block group
   * @param storedBlock current storedBlock which is either a
   *                    BlockInfoContiguous or a BlockInfoStriped
   */
  void incrementSafeBlockCount(int storageNum, BlockInfo storedBlock);

  /**
   * Decrement number of safe blocks if the current block is contiguous
   * and it has just fallen below minimal replication or
   * if the current block is striped and its actual data blocks has just fallen
   * below the number of data units specified by erasure coding policy.
   * If safe mode is not currently on, this is a no-op.
   */
  void decrementSafeBlockCount(BlockInfo b);

  /**
   * Check if the block report replica has a generation stamp (GS) in future.
   * If safe mode is not currently on, this is a no-op.
   *
   * @param brr block report replica which belongs to no file in BlockManager
   */
  void checkBlocksWithFutureGS(BlockReportReplica brr);

  /**
   * Returns the number of bytes that reside in blocks with Generation Stamps
   * greater than generation stamp known to Namenode.
   *
   * @return Bytes in future
   */
  long getBytesInFuture();

  /**
   * Returns the number of bytes that reside in continous blocks with Generation Stamps
   * greater than generation stamp known to Namenode.
   *
   * @return Bytes in future
   */
  long getBytesInFutureBlocks();

  /**
   * Returns the number of bytes that reside in EC blocks with Generation Stamps
   * greater than generation stamp known to Namenode.
   *
   * @return Bytes in future
   */
  long getBytesInFutureECBlockGroups();
}
