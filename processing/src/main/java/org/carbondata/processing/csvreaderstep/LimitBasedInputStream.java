/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.carbondata.processing.csvreaderstep;

import java.io.IOException;
import java.io.InputStream;

/**
 * Decorator over input stream which will be used to
 * read the data from. It is a size based stream which
 * can be used to read the file until limit is reached
 */
public class LimitBasedInputStream extends InputStream {

  /**
   * steam which will be used to
   * read the data from file
   */
  private InputStream in;

  /**
   * maximum number of bytes this stream can read
   */
  private long maximumBytesToRead;

  /**
   * current byte read
   */
  private long currentBytesRead;

  public LimitBasedInputStream(InputStream in, long maxBytesToRead) {
    this.in = in;
    this.maximumBytesToRead = maxBytesToRead;
  }

  /**
   * Method to read the data byte by byte
   *
   * @return number of actual bytes read
   */
  @Override public int read() throws IOException {
    if (currentBytesRead >= maximumBytesToRead) {
      return -1;
    }
    int b = in.read();
    if (b != -1) {
      currentBytesRead++;
    }
    return b;
  }

  /**
   * Below method will be used to read the data from stream
   * it will keep track of how many byte it has read, when threshold is
   * reach it will return -1
   *
   * @param buffer buffer to be filled
   * @return number of actual bytes read
   */
  @Override public int read(byte[] b) throws IOException {
    return this.read(b, 0, b.length);
  }

  /**
   * Below method will be used to read the data from stream
   * it will keep track of how many byte it has read, when threshold is
   * reach it will break
   *
   * @param buffer buffer to be filled
   * @param offset position from which data will be filled in the buffer
   * @param len    number of bytes to be read
   * @return number of actual bytes read
   */
  @Override public int read(byte[] buffer, int off, int len) throws IOException {
    // if max number of bytes assigned to this stream is reached then return -1
    // to specify no more bytes is present in file
    if (currentBytesRead == maximumBytesToRead) {
      return -1;
    }
    // number of bytes left
    long bytesLeft = getBytesLeft();
    if (len > bytesLeft) {
      len = (int) bytesLeft;
    }
    int bytesJustRead = in.read(buffer, off, len);
    currentBytesRead += bytesJustRead;
    return bytesJustRead;
  }

  /**
   * method to close the stream
   */
  @Override public void close() throws IOException {
    in.close();
  }

  /**
   * @return number of bytes available for reading
   */
  public long getBytesLeft() {
    return maximumBytesToRead - currentBytesRead;
  }
}