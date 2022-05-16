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

package org.apache.hadoop.hdfs.server.lock.exception;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;
import java.text.MessageFormat;

/**
 * Exception messages used across lock.
 *
 * Note: To minimize merge conflicts, please sort alphabetically in this section.
 */
@ThreadSafe
public enum ExceptionMessage {
  // general
  INVALID_PREFIX("Parent path \"{0}\" is not a prefix of child {1}."),
  NOT_SUPPORTED("This method is not supported."),
  PATH_DOES_NOT_EXIST("Path \"{0}\" does not exist."),
  PATH_MUST_BE_FILE("Path \"{0}\" must be a file."),
  PATH_MUST_BE_DIRECTORY("Path \"{0}\" must be a directory."),
  PATH_INVALID("Path \"{0}\" is invalid."),

  // file system
  NOT_MUTABLE_INODE_PATH("Not a MutableLockedInodePath: {0}"),
  PATH_COMPONENTS_INVALID("Parameter pathComponents is {0}"),
  PATH_COMPONENTS_INVALID_START("Path starts with {0}"),
  PATH_INVALID_CONCURRENT_RENAME("Path is no longer valid, possibly due to a concurrent rename."),
  PATH_INVALID_CONCURRENT_DELETE("Path is no longer valid, possibly due to a concurrent delete."),
  PATH_MUST_HAVE_VALID_PARENT("{0} does not have a valid parent"),
  INODE_DOES_NOT_EXIST("inodeId {0,number,#} does not exist"),

  // SEMICOLON! minimize merge conflicts by putting it on its own line
  ;

  private final MessageFormat mMessage;

  ExceptionMessage(String message) {
    mMessage = new MessageFormat(message);
  }

  /**
   * Formats the message of the exception.
   *
   * @param params the parameters for the exception message
   * @return the formatted message
   */
  public String getMessage(Object... params) {
    Preconditions.checkArgument(mMessage.getFormats().length == params.length,
        "The message takes " + mMessage.getFormats().length + " arguments, but is given "
            + params.length);
    // MessageFormat is not thread-safe, so guard it
    synchronized (mMessage) {
      return mMessage.format(params);
    }
  }

  /**
   * Formats the message of the exception with a url to consult.
   *
   * @param url the url to consult
   * @param params the parameters for the exception message
   * @return the formatted message
   */
  public String getMessageWithUrl(String url, Object... params) {
    return getMessage(params) + " Please consult " + url
        + " for common solutions to address this problem.";
  }
}
