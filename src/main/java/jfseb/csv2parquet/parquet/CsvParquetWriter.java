/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jfseb.csv2parquet.parquet;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;

import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import jfseb.csv2parquet.convert.CsvWriteSupport;

public class CsvParquetWriter extends ParquetWriter<List<String>> {

	public CsvParquetWriter(Path file, MessageType schema) throws IOException {
		this(file, schema, false, false);
	}

	public CsvParquetWriter(Path file, MessageType schema, boolean enableDictionary, boolean readAsBinary) throws IOException {
		this(file, schema, CompressionCodecName.UNCOMPRESSED, enableDictionary, readAsBinary);
	}

	public CsvParquetWriter(Path file, MessageType schema, CompressionCodecName codecName, boolean enableDictionary, boolean readAsBinary)
			throws IOException {
		super(file, (WriteSupport<List<String>>) new CsvWriteSupport(schema, readAsBinary), codecName, DEFAULT_BLOCK_SIZE,
				DEFAULT_PAGE_SIZE, enableDictionary, false);
	}

	public CsvParquetWriter(Path file, MessageType schema, CompressionCodecName codecName, int block_size,
			int page_size, boolean enableDictionary, boolean readAsBinary) throws IOException {
		super(file, (WriteSupport<List<String>>) new CsvWriteSupport(schema, readAsBinary), codecName, block_size, page_size,
				enableDictionary, false);

	}
}
