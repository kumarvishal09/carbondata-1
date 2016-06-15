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

import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;

/**
 * Decorator over input stream which will be used to read the data from. It is a
 * size based stream which can be used to read the file until limit is reached
 */
public class LimitBasedInputStream extends InputStream {

	private static final byte END_OF_LINE_BYTE_VALUE = (byte) '\n';
	/**
	 * steam which will be used to read the data from file
	 */
	private InputStream in;

	/**
	 * maximum number of bytes this stream can read
	 */
	private long maxNumberOfBytesToRead;

	/**
	 * current byte read
	 */
	private long currentBytesRead;

	private byte[] data;

	private boolean isFirst;

	private boolean needToReadExtraLine;
	
//	public static void main(String[] args) throws IOException {
//		
//		long offset=0;
//		long endOffset=2113929216;
//		long lastLength=11023195;		
//		int counter=0;
//		while(offset<endOffset)
//		{
//			if(counter>2)
//			{
//				break;
//			}
//			LimitBasedInputStream limitBasedInputStream = new LimitBasedInputStream(33554432, true);
//			limitBasedInputStream.initialize(50000, offset, "D:/OsconData/20160515/001001.csv");
////			byte[] b = new byte[50000];
////			while(true)
////			{
////				int read = limitBasedInputStream.read(b, 0, 50000);
////				if(read==-1)
////				{
////					break;
////				}
////			}
//			offset+=33554432;
//			counter++;
//		}
//	}
	public LimitBasedInputStream(long maxNumberOfBytesToRead,
			boolean needToReadExtraLine) {
		this.maxNumberOfBytesToRead = maxNumberOfBytesToRead;
		this.needToReadExtraLine = needToReadExtraLine;
	}

	public void initialize(int bufferSize, long offset, String path)
			throws IOException {
		FileType fileType = FileFactory.getFileType(path);
		if (offset != 0) {
			isFirst = true;
			offset = offset - 1;
		}
		in = FileFactory.getDataInputStream(path, fileType, bufferSize, offset);
		if (offset == 0) {
			return;
		}
		boolean isFound = false;
		int counter = 0;
		int read = 0;
		data = new byte[1000];
		int numberOfBytesToSkip=0;
		while (true) {
			read = in.read(data, 0, 1000);
			counter = 0;
			while (counter < read) {
				if (data[counter++] == END_OF_LINE_BYTE_VALUE) {
					isFound = true;
					break;
				}
				numberOfBytesToSkip++;
			}
			if (isFound) {
				break;
			}
		}
		System.out.println(offset+numberOfBytesToSkip);
		byte[] newData = new byte[read - counter];
		System.arraycopy(data, counter, newData, 0, newData.length);
		data = newData;
		maxNumberOfBytesToRead=maxNumberOfBytesToRead-numberOfBytesToSkip;
		System.out.println(maxNumberOfBytesToRead);
	}

	/**
	 * Below method will be used to read the data from stream it will keep track
	 * of how many byte it has read, when threshold is reach it will break
	 *
	 * @param buffer
	 *            buffer to be filled
	 * @param offset
	 *            position from which data will be filled in the buffer
	 * @param len
	 *            number of bytes to be read
	 * @return number of actual bytes read
	 */
	@Override
	public int read(byte[] buffer, int off, int len) throws IOException {
		if (isFirst) {
			isFirst = false;
			System.arraycopy(data, 0, buffer, off, data.length);
			return data.length;
		}
		// if max number of bytes assigned to this stream is reached then return
		// -1
		// to specify no more bytes is present in file
		if (currentBytesRead == maxNumberOfBytesToRead) {
			if (needToReadExtraLine) {
				int counter = 0;
				int read = 0;
				data = new byte[1000];
				read = in.read(data, 0, 1000);
				counter = 0;
				while (counter < read) {
					if (data[counter++] == END_OF_LINE_BYTE_VALUE) {
						needToReadExtraLine = false;
						break;
					}
				}
				System.arraycopy(data, 0, buffer, off, counter);
				return counter;
			}
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
	@Override
	public void close() throws IOException {
		in.close();
	}

	/**
	 * @return number of bytes available for reading
	 */
	public long getBytesLeft() {
		return maxNumberOfBytesToRead - currentBytesRead;
	}

	/**
	 * Method to read the data byte by byte
	 *
	 * @return number of actual bytes read
	 */
	@Override
	public int read() throws IOException {
		throw new UnsupportedOperationException(
				"Below operation is not supported");
	}

	/**
	 * Below method will be used to read the data from stream it will keep track
	 * of how many byte it has read, when threshold is reach it will return -1
	 *
	 * @param buffer
	 *            buffer to be filled
	 * @return number of actual bytes read
	 */
	@Override
	public int read(byte[] b) throws IOException {
		throw new UnsupportedOperationException(
				"Below operation is not supported");
	}
}