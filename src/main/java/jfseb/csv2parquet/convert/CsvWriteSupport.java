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
package jfseb.csv2parquet.convert;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.util.TimestampUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.example.data.simple.Int96Value;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import jfseb.csv2parquet.convert.utils.CSV2ParquetTimestampUtils;
import jfseb.csv2parquet.utils.ParseHexRec;

//import parquet.example.data.simple.NanoTime;

public class CsvWriteSupport extends WriteSupport<List<String>> {
  MessageType schema;
  RecordConsumer recordConsumer;
  List<ColumnDescriptor> cols;
  boolean readAsBinary;

  // TODO: support specifying encodings and compression
  public CsvWriteSupport(MessageType schema, boolean readAsBinary) {
    this.schema = schema;
    this.cols = schema.getColumns();
    this.readAsBinary = readAsBinary;
  }

  @Override
  public WriteContext init(Configuration config) {
    return new WriteContext(schema, new HashMap<String, String>());
  }

  @Override
  public void prepareForWrite(RecordConsumer r) {
    recordConsumer = r;
  }

  @Override
  public void write(List<String> values) {
    if (values.size() != cols.size()) {
      throw new ParquetEncodingException("Invalid input data. Expecting " + cols.size() + " columns. Input had "
          + values.size() + " columns (" + cols + ") : " + values);
    }

    recordConsumer.startMessage();
    for (int i = 0; i < cols.size(); ++i) {
      String val = values.get(i);
      // val.length() == 0 indicates a NULL value.
      if (val.length() > 0) {
        recordConsumer.startField(cols.get(i).getPath()[0], i);
        try {
          ParseHexRec.ParsedRec rec = null;
          if (this.readAsBinary) {
            rec = ParseHexRec.parse(val);
          }
          Type primtype = schema.getFields().get(i);
          switch (cols.get(i).getType()) {
          case BOOLEAN:
            if (rec != null) {
              recordConsumer.addBoolean(rec.asBool);
            } else {
              recordConsumer.addBoolean(Boolean.parseBoolean(val));
            }
            break;
          case INT96:
            if (rec != null) {
              Binary bin = rec.getBinary(96 / 8);
              recordConsumer.addBinary(bin);
            } else {
              try {
                DateFormat df = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss"); // , Locale.ENGLISH);
                java.util.Date result = df.parse(val);
                NanoTime nt = CSV2ParquetTimestampUtils.fromDateTimeString(val);
                // try a byte array a la
                // https://www.programcreek.com/java-api-examples/index.php?source_dir=presto-master/presto-hive/src/test/java/com/facebook/presto/hive/parquet/TestParquetTimestampUtils.java
                // todo : parse millis

                // org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils;
                // ts.getTime();
                // nt.getBinary();
                // Timestamp timestamp = Timestamp.valueOf(val); // timestampString);
                // (( Binary timestampBytes = NanoTimeUtils.getNanoTime(timestamp,
                // false).toBinary();
                // long decodedTimestampMillis = getTimestampMillis(timestampBytes);
                // assertEquals(decodedTimestampMillis, timestamp.getTime());
                // Int96Value i96 = new Int96Value(Binar));
                recordConsumer.addBinary(nt.toBinary());
                // recordConsumer.addLong(result.getTime());
              } catch (java.text.ParseException ex) {
                recordConsumer.addLong(Long.parseLong(val));
              }
            }
            break;
          case FLOAT:
            if (rec != null) {
              recordConsumer.addFloat(rec.asFloat);
            } else {
              recordConsumer.addFloat(Float.parseFloat(val));
            }
            break;
          case DOUBLE:
            if (rec != null) {
              recordConsumer.addDouble(rec.asDouble);
            } else {
              recordConsumer.addDouble(Double.parseDouble(val));
            }
            break;
          case INT32:
            if (rec != null) {
              recordConsumer.addInteger(rec.asInt);
            } else {
              if (primtype.getOriginalType() == OriginalType.DATE) {
                val = CSV2ParquetTimestampUtils.parseDateOrIntSloppy(val);
              } else if (primtype.getOriginalType() == OriginalType.TIME_MILLIS) {
                val = Integer.valueOf(CSV2ParquetTimestampUtils.parseTimeMillisOrInt(val)).toString();
              }
              recordConsumer.addInteger(Integer.parseInt(val));
            }
            break;
          case INT64:
            if (rec != null) {
              recordConsumer.addLong(rec.asLong);
            } else {
              if (primtype.getOriginalType() == OriginalType.TIME_MICROS) {
                try {
                  long valmicros = CSV2ParquetTimestampUtils.parseTimeMicros(val, false);
                  recordConsumer.addLong(valmicros);
                } catch (ParseException e) {
                  recordConsumer.addLong(Long.parseLong(val));
                }
              } /*
                 * try { DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // ,
                 * Locale.ENGLISH); java.util.Date result = df.parse(val);
                 * recordConsumer.addLong(result.getTime()); } catch (java.text.ParseException
                 * ex) { recordConsumer.addLong(Long.parseLong(val)); }
                 */
              else {
                recordConsumer.addLong(Long.parseLong(val));
              }
            }
            break;
          case BINARY:
            if (rec != null) {
              recordConsumer.addBinary(rec.binary);
            } else {
              recordConsumer.addBinary(stringToBinary(val));
            }
            break;
          case FIXED_LEN_BYTE_ARRAY:
            if (rec != null) {
              recordConsumer.addBinary(rec.binary);
            } else {
              recordConsumer.addBinary(stringToBinary(val));
            }
            break;
          default:
            throw new ParquetEncodingException("Unsupported column type: " + cols.get(i).getType());
          }
        } catch (java.lang.NumberFormatException e) {
          throw new IllegalArgumentException("column nr:" + i + " \"" + cols.get(i).getPath()[0] + "\" typed as "
              + cols.get(i).getType() + " \n value: \"" + val + "\"", e);
        }
        recordConsumer.endField(cols.get(i).getPath()[0], i);
      }
    }
    recordConsumer.endMessage();
  }

  public void writeBinary(List<String> values) {
    if (values.size() != cols.size()) {
      throw new ParquetEncodingException("Invalid input data. Expecting " + cols.size() + " columns. Input had "
          + values.size() + " columns (" + cols + ") : " + values);
    }

    recordConsumer.startMessage();
    for (int i = 0; i < cols.size(); ++i) {
      String val = values.get(i);
      // val.length() == 0 indicates a NULL value.
      if (val.length() > 0) {
        ParseHexRec.ParsedRec rec = ParseHexRec.parse(val);

        recordConsumer.startField(cols.get(i).getPath()[0], i);
        try {
          Type primtype = schema.getFields().get(i);
          switch (cols.get(i).getType()) {
          case BOOLEAN:
            recordConsumer.addBoolean(rec.asBool);
            break;
          case INT96:
            if (rec.binary.length() < 96 / 8) {
              throw new IllegalArgumentException(" binary too short for int96");
            }
            Binary bin = rec.binary.slice(rec.binary.length() - 96 / 8, rec.binary.length());
            recordConsumer.addBinary(bin);
            break;
          case FLOAT:
            recordConsumer.addFloat(rec.asFloat);
            break;
          case DOUBLE:
            recordConsumer.addDouble(rec.asDouble);
            break;
          case INT32:
            recordConsumer.addInteger(rec.asInt);
            break;
          case INT64:
            recordConsumer.addLong(rec.asLong);
            break;
          case BINARY:
            recordConsumer.addBinary(rec.binary);
            break;
          case FIXED_LEN_BYTE_ARRAY:
            recordConsumer.addBinary(rec.binary);
          default:
            throw new ParquetEncodingException("Unsupported column type: " + cols.get(i).getType());
          }
        } catch (java.lang.NumberFormatException e) {
          throw new IllegalArgumentException("column nr:" + i + " \"" + cols.get(i).getPath()[0] + "\" typed as "
              + cols.get(i).getType() + " \n value: \"" + val + "\"", e);
        }
        recordConsumer.endField(cols.get(i).getPath()[0], i);
      }
    }
    recordConsumer.endMessage();
  }

  private Binary stringToBinary(Object value) {
    return Binary.fromString(value.toString());
  }
}
