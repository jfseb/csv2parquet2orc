/*
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
package jfseb.csv2parquet.convert;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription.Category;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;

import jfseb.csv2parquet.Utils;
import jfseb.csv2parquet.parquet.CsvParquetWriter;
import jfseb.csv2parquet.parquet.CsvTypedParquetWriter;
import jfseb.csv2parquet.utils.SchemaCreator;
import jfseb.csv2parquet.utils.USchema;

/**
 * A conversion tool to convert CSV or JSON files into ORC files.
 */
public class ConvertToolParquet extends ConvertToolBase {

  /*
   * TypeDescription buildSchema(List<FileInformation> files, Configuration conf)
   * throws IOException { JsonSchemaFinder schemaFinder = new JsonSchemaFinder();
   * int filesScanned = 0; for(FileInformation file: files) { if (file.format ==
   * Format.JSON) { System.err.println("Scanning " + file.path + " for schema");
   * filesScanned += 1;
   * schemaFinder.addFile(file.getReader(file.filesystem.open(file.path))); } else
   * if (file.format == Format.ORC) { System.err.println("Merging schema from " +
   * file.path); filesScanned += 1; Reader reader =
   * OrcFile.createReader(file.path, OrcFile.readerOptions(conf)
   * .filesystem(file.filesystem)); schemaFinder.addSchema(reader.getSchema()); }
   * } if (filesScanned == 0) { throw new
   * IllegalArgumentException("Please specify a schema using" +
   * " --schema for converting CSV files."); } return schemaFinder.getSchema(); }
   */

  public static void main(Configuration conf, String[] args) throws IOException, ParseException {
    new ConvertToolParquet(conf, args).run();
  }

  private USchema uschema;

  public ConvertToolParquet(Configuration conf, String[] args) throws IOException, ParseException {
    super(conf, args, Format.PARQUET);
  }

  // writer = OrcFile.createWriter(new Path(outFilename),
  // OrcFile.writerOptions(conf).setSchema(schema));
  // batch = schema.createRowBatch();

  void run() throws IOException {
    for (FileInformation file : fileList) {
      System.err.println("Processing " + file.getPath());
      java.io.File csvFile = new java.io.File(file.getPath().toString());
      ConvertUtils.convertCsvToParquet(csvFile, new java.io.File(this.outFileName), this.schemaString,
          conf.getBoolean("parquet.enabledictionary", false), this.csvOptions, this.conf);// ,
    }
  }

  void run2() throws IOException {

    boolean enableDictionary = false;

    Path path = new Path(this.outFileName);
    CompressionCodecName codecName = CompressionCodecName.UNCOMPRESSED;
    int block_size = org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
    int page_size = org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;
    if (conf != null) {
      String cmdline = conf.get("parquet.compress", "GZIP");
      if ("ZIP".equals(cmdline) || "GZIP".equals(cmdline)) {
        codecName = CompressionCodecName.GZIP;
      } else if ("SNAPPY".equals(cmdline)) {
        codecName = CompressionCodecName.SNAPPY;
      } else if ("NONE".equals(cmdline) || "UNCOMPRESSED".equals(cmdline)) {
        codecName = CompressionCodecName.UNCOMPRESSED;
      } else {
        throw new IllegalArgumentException(" parquet.compress must be [ZIP=GZIP, SNAPPY, NONE=UNCOMPRESSED]");
      }
      block_size = conf.getInt("parquet.BLOCK_SIZE", org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE);
      page_size = conf.getInt("parquet.PAGE_SIZE", org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE);
      enableDictionary = conf.getBoolean("parquet.enabledictionary", enableDictionary);
    }

    VectorizedRowBatch batch;
    this.uschema = SchemaCreator.makeSchema(new java.io.File(fileList.get(0).getPath().toString()), this.schemaString);

    CsvTypedParquetWriter writer = new CsvTypedParquetWriter(path, this.uschema.messageType, codecName, block_size,
        page_size, enableDictionary);

    try {
      for (FileInformation file : fileList) {
        System.err.println("Processing " + file.getPath());
        java.io.File csvFile = new java.io.File(file.getPath().toString());
        this.uschema = SchemaCreator.makeSchema(csvFile, this.schemaString);

        batch = this.uschema.typeDescription.createRowBatch();
        System.err.println("Processing " + file.getPath());
        RecordReader reader = file.getRecordReader(this.uschema.typeDescription);
        while (reader.nextBatch(batch)) {
          for (int n = 0; n < batch.count(); ++n) {
            ArrayList<Object> rec = new ArrayList<Object>(batch.cols.length);
            for (int i = 0; i < this.uschema.typeDescription.getChildren().size(); ++i) {
              ColumnVector cv = batch.cols[i];
              if (cv.isNull[n]) {
                rec.add(null);
              }
              Category colcategory = this.uschema.typeDescription.getChildren().get(i).getCategory();
              switch (colcategory) {

              case BOOLEAN: {
                // return new BooleanConverter(startOffset); // long column!
                LongColumnVector bcs = (LongColumnVector) cv;
                rec.add(Boolean.valueOf(0 != bcs.vector[n]));
              }
                break;
              case BYTE:
              case SHORT:
                LongColumnVector bcsS = (LongColumnVector) cv;
                rec.add(Long.valueOf(bcsS.vector[n]));
              case INT: {
                LongColumnVector bcsI = (LongColumnVector) cv;
                rec.add(Integer.valueOf((int) bcsI.vector[n]));
              }
                break;
              case LONG: {
                LongColumnVector bcs = (LongColumnVector) cv;
                rec.add(Long.valueOf(bcs.vector[n]));
                // return new LongConverter(startOffset);
              }
                break;
              case FLOAT: {
                DoubleColumnVector bcs = (DoubleColumnVector) cv;
                rec.add(Float.valueOf((float) bcs.vector[n]));
              }
                break;
              case DOUBLE: {
                DoubleColumnVector bcs = (DoubleColumnVector) cv;
                rec.add(Double.valueOf(bcs.vector[n]));
              }
                break;
              // return new DoubleConverter(startOffset);
              case DECIMAL: {
                BytesColumnVector bcs = (BytesColumnVector) cv;
                rec.add(Binary.fromConstantByteArray(bcs.vector[n])); // is this constant?
                /*
                 * return new DecimalConverter(startOffset);
                 */
              }
                break;
              case BINARY:
              case STRING:
              case CHAR:
              case VARCHAR: {
                BytesColumnVector bcs = (BytesColumnVector) cv;
                rec.add(Binary.fromConstantByteArray(bcs.vector[n])); // is this constant?
                /*
                 * return new BytesConverter(startOffset);
                 */
              }
                break;
              case TIMESTAMP: {
                TimestampColumnVector dcs = (TimestampColumnVector) cv;
                rec.add(Timestamp.from(Instant.ofEpochMilli(dcs.getTimestampAsLong(n)))); // QueSTIONABLE
                // return new TimestampConverter(startOffset);
              }
                break;
              default:
                throw new IllegalArgumentException(
                    "unknown category in column " + Integer.toString(i) + " " + colcategory);
              }
            }
            /*
             * else if (cv instanceof DoubleColumnVector) { DoubleColumnVector dcs =
             * (DoubleColumnVector) cv; rec.add(Double.valueOf(dcs.vector[n])); } else if
             * (cv instanceof TimestampColumnVector) { TimestampColumnVector dcs =
             * (TimestampColumnVector) cv;
             * rec.add(Timestamp.from(Instant.ofEpochMilli(dcs.getTimestampAsLong(n)))); //
             * QueSTIONABLE } else if (cv instanceof BytesColumnVector) { BytesColumnVector
             * bcs = (BytesColumnVector) cv;
             * rec.add(Binary.fromConstantByteArray(bcs.vector[n])); // is this constant? }
             * else if (cv instanceof LongColumnVector) { LongColumnVector bcs =
             * (LongColumnVector) cv; rec.add(Long.valueOf(bcs.vector[n])); }
             */
            writer.write(rec);
          }
        }
        reader.close();
      }
    } finally

    {
      Utils.closeQuietly(writer);
    }
  }
}
