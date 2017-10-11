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
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.example.data.simple.Int96Value;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

//import parquet.example.data.simple.NanoTime;

public class CsvTypedWriteSupport extends WriteSupport<List<Object>> {
  MessageType schema;
  RecordConsumer recordConsumer;
  List<ColumnDescriptor> cols;

  // TODO: support specifying encodings and compression
  public CsvTypedWriteSupport(MessageType schema) {
    this.schema = schema;
    this.cols = schema.getColumns();
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
  public void write(List<Object> values) {
    if (values.size() != cols.size()) {
      throw new ParquetEncodingException("Invalid input data. Expecting " + cols.size() + " columns. Input had "
          + values.size() + " columns (" + cols + ") : " + values);
    }
    
      /*
  case BOOLEAN:
    return new BooleanConverter(startOffset);
  case BYTE:
  case SHORT:
  case INT:
  case LONG:
    return new LongConverter(startOffset);
  case FLOAT:
  case DOUBLE:
    return new DoubleConverter(startOffset);
  case DECIMAL:
    return new DecimalConverter(startOffset);
  case BINARY:
  case STRING:
  case CHAR:
  case VARCHAR:
    return new BytesConverter(startOffset);
  case TIMESTAMP:
    return new TimestampConverter(startOffset);
    */

    recordConsumer.startMessage();
    for (int i = 0; i < cols.size(); ++i) {
      Object val = values.get(i);
      // val.length() == 0 indicates a NULL value.
      if (val != null) {
        recordConsumer.startField(cols.get(i).getPath()[0], i);
        try {
          switch (cols.get(i).getType()) {
          case BOOLEAN:
            recordConsumer.addBoolean(val == Boolean.TRUE);
            break;
          case INT96:
            if (val instanceof Timestamp) {
              Timestamp ts = (Timestamp) val;
              recordConsumer.addLong(((Timestamp) val).getTime());
              Binary binary;
              byte[] bts = new byte[12];
              for (i = 0; i < bts.length; ++i) {
                bts[i] = 0;
              }
              binary = Binary.fromConstantByteArray(bts);
              Int96Value iv = new Int96Value(binary);
              recordConsumer.addBinary(binary);
            }
            if (val instanceof Int96Value) {
              Int96Value int96 = (Int96Value) val;
              int96.getBinary();
              recordConsumer.addBinary(int96.getBinary());
            }
            break;
          case FLOAT:
            Float valFloat = (Float) val;
            recordConsumer.addFloat(valFloat.floatValue());
            break;
          case DOUBLE:
            Double valDouble = (Double) val;
            recordConsumer.addDouble(valDouble.doubleValue());
            break;
          case INT32:
            recordConsumer.addInteger(((Integer) val).intValue());
            break;
          case INT64:
            // TODO DATE TIME

            recordConsumer.addLong(((Long) val).longValue());
            /*
             * try { DateFormat df = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss"); //,
             * Locale.ENGLISH); java.util.Date result = df.parse(val);
             * recordConsumer.addLong(result.getTime()); } catch ( java.text.ParseException
             * ex ) { recordConsumer.addLong(Long.parseLong(val)); }
             */
            break;
          case BINARY:
            recordConsumer.addBinary(stringToBinary(val));
            break;
          case FIXED_LEN_BYTE_ARRAY:
            if (val instanceof String) {
              recordConsumer.addBinary(stringToBinary(val));
            }
            if (val instanceof Binary) {
              recordConsumer.addBinary((Binary) val);
            }
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
