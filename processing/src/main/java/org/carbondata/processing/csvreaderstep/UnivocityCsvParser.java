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
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

/**
 * Class which will be used to read the data from csv file and parse the record
 */
public class UnivocityCsvParser {

	/**
	 * reader for csv
	 */
	private Reader inputStreamReader;

	/**
	 * buffer size of stream
	 */
	private int bufferSize;

	/**
	 * to keep track how many block has been processed
	 */
	private int blockCounter = -1;

	/**
	 * csv record parser which read and convert the record to csv format
	 */
	private CsvParser parser;

	/**
	 * row from csv
	 */
	private String[] row;

	/**
	 * holding all the properties required for parsing the records
	 */
	private UnivocityCsvParserVo csvParserVo;

	public UnivocityCsvParser(UnivocityCsvParserVo csvParserVo) {
		this.csvParserVo = csvParserVo;
		bufferSize = Integer.parseInt(CarbonProperties.getInstance()
				.getProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE,
						CarbonCommonConstants.CSV_READ_BUFFER_SIZE_DEFAULT));
	}

	/**
	 * Below method will be used to initialize the the parser
	 *
	 * @throws IOException
	 */
	public void initialize() throws IOException {
		CsvParserSettings parserSettings = new CsvParserSettings();
		parserSettings.getFormat().setLineSeparator(csvParserVo.getNewLine());
		parserSettings.getFormat().setDelimiter(
				csvParserVo.getDelimiter().charAt(0));
		parserSettings.setMaxColumns(csvParserVo.getNumberOfColumns() + 10);
		parserSettings.setNullValue("");
		blockCounter++;
		initializeReader();
		if (csvParserVo.getBlockDetailsList().get(blockCounter)
				.getBlockOffset() == 0) {
			parserSettings.setHeaderExtractionEnabled(csvParserVo
					.isHeaderPresent());
		}
		parser = new CsvParser(parserSettings);
		parser.beginParsing(inputStreamReader);
	}

	/**
	 * Below method will be used to initialize the reader
	 *
	 * @throws IOException
	 */
	private void initializeReader() throws IOException {
		// if already one input stream is open first we need to close and then
		// open new stream
		close();
		// get the block length
		long blockLength = this.csvParserVo.getBlockDetailsList()
				.get(blockCounter).getBlockLength();
		// get the block offset
		long blockOffset = this.csvParserVo.getBlockDetailsList()
				.get(blockCounter).getBlockOffset();
		// get the actual file size
		long actualFileSize = FileFactory.getCarbonFile(
				this.csvParserVo.getBlockDetailsList().get(blockCounter)
						.getFilePath(),
				FileFactory.getFileType(this.csvParserVo.getBlockDetailsList()
						.get(blockCounter).getFilePath())).getSize();
		boolean needToReadExtraLine = false;
		// if it is not the last block of the file then we need
		// to skip some of the bytes so one complete record will be read
		if (!((blockLength + blockOffset) >= actualFileSize)) {
			needToReadExtraLine = true;
		}
		LimitBasedInputStream limitBasedInputStream = new LimitBasedInputStream(
				blockLength, needToReadExtraLine);
		limitBasedInputStream.initialize(bufferSize, blockOffset,
				this.csvParserVo.getBlockDetailsList().get(blockCounter)
						.getFilePath());
		inputStreamReader = new InputStreamReader(limitBasedInputStream,
				Charset.defaultCharset());

	}

	/**
	 * Below method will be used to clear all the stream
	 */
	public void close() {
		if (null != inputStreamReader) {
			CarbonUtil.closeStreams(inputStreamReader);
		}

	}

	/**
	 * Below method will be used to check whether any more records is present or
	 * not
	 *
	 * @return true if more records are present
	 * @throws IOException
	 */
	public boolean hasMoreRecords() throws IOException {
		row = parser.parseNext();
		if (row == null
				&& blockCounter + 1 >= this.csvParserVo.getBlockDetailsList()
						.size()) {
			return false;
		}
		if (row == null) {
			initialize();
			row = parser.parseNext();
		}
		return true;
	}

	/**
	 * Below method will be used to get the new record
	 *
	 * @return next record
	 */
	public String[] getNextRecord() {
		String[] returnValue = row;
		row = null;
		return returnValue;
	}
}
