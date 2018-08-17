package jfseb.prevorc15.org.apache.orc.tools.convert;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.parquet.Log;
import org.threeten.bp.LocalDateTime;
import org.threeten.bp.ZoneId;
import org.threeten.bp.ZonedDateTime;
import org.threeten.bp.format.DateTimeFormatter;
import org.threeten.bp.format.DateTimeParseException;
import org.threeten.bp.temporal.TemporalAccessor;

import com.opencsv.CSVReader;

import jfseb.csv2parquet.convert.ConvertToolBase.CSVOptions;
import jfseb.csv2parquet.convert.utils.CSV2ParquetTimestampUtils;
import jfseb.csv2parquet.utils.ParseHexRec;

import java.io.IOException;
import java.io.Reader;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;

public class CsvReader implements RecordReader {
  private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter
      .ofPattern("yyyy[[-][/]]MM[[-][/]]dd[['T'][ ]]HH:mm:ss[ ][XXX][X]");

  private long rowNumber = 0;
  private final Converter converter;
  private final int columns;
  private final CSVReader reader;
  private final String nullString;
  private final FSDataInputStream underlying;
  private final long totalSize;

  public TypeDescription schema;

  private final boolean csvFormatBinary;

  /**
   * Create a CSV reader
   * 
   * @param reader
   *          the stream to read from
   * @param input
   *          the underlying file that is only used for getting the position
   *          within the file
   * @param size
   *          the number of bytes in the underlying stream
   * @param schema
   *          the schema to read into
   * @param separatorChar
   *          the character between fields
   * @param quoteChar
   *          the quote character
   * @param escapeChar
   *          the escape character
   * @param headerLines
   *          the number of header lines
   * @param nullString
   *          the string that is translated to null
   * @throws IOException
   */
  public CsvReader(java.io.Reader reader, FSDataInputStream input, long size, TypeDescription schema,
      CSVOptions csvoptions) throws IOException {
    this.underlying = input;
    this.schema = schema;
    this.reader = new CSVReader(reader, csvoptions.csvSeparatorAsChar, csvoptions.csvQuote, csvoptions.csvEscape,
        csvoptions.csvHeaderLines);
    this.nullString = csvoptions.csvNullString;
    this.csvFormatBinary = csvoptions.csvFormatBinary;
    this.totalSize = size;
    IntWritable nextColumn = new IntWritable(0);
    this.converter = buildConverter(nextColumn, schema);
    this.columns = nextColumn.get();
  }

  interface Converter {
    void convert(String[] values, VectorizedRowBatch batch, int row);

    void convert(String[] values, ColumnVector column, int row);
  }

  @Override
  public boolean nextBatch(VectorizedRowBatch batch) throws IOException {
    batch.reset();
    final int BATCH_SIZE = batch.getMaxSize();
    String[] nextLine;
    // Read the CSV rows and place them into the column vectors.
    while ((nextLine = reader.readNext()) != null) {
      rowNumber++;
      if (nextLine.length != columns && !(nextLine.length == columns + 1 && "".equals(nextLine[columns]))) {
        throw new IllegalArgumentException(
            "Too many columns on line " + rowNumber + ". Expected " + columns + ", but got " + nextLine.length + ".");
      }
      converter.convert(nextLine, batch, batch.size++);
      if (batch.size == BATCH_SIZE) {
        break;
      }
    }
    return batch.size != 0;
  }

  @Override
  public long getRowNumber() throws IOException {
    return rowNumber;
  }

  @Override
  public float getProgress() throws IOException {
    long pos = underlying.getPos();
    return totalSize != 0 && pos < totalSize ? (float) pos / totalSize : 1;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public void seekToRow(long rowCount) throws IOException {
    throw new UnsupportedOperationException("Seeking not supported");
  }

  abstract class ConverterImpl implements Converter {
    final int offset;

    ConverterImpl(IntWritable offset) {
      this.offset = offset.get();
      offset.set(this.offset + 1);
    }

    @Override
    public void convert(String[] values, VectorizedRowBatch batch, int row) {
      convert(values, batch.cols[0], row);
    }
  }

  class BooleanConverter extends ConverterImpl {
    BooleanConverter(IntWritable offset) {
      super(offset);
    }

    @Override
    public void convert(String[] values, ColumnVector column, int row) {
      if (values[offset] == null || nullString.equals(values[offset])) {
        column.noNulls = false;
        column.isNull[row] = true;
      } else {
        ParseHexRec.ParsedRec rec = ParseHexRec.parse(values[offset]);
        if (rec != null && CsvReader.this.csvFormatBinary) {
          ((LongColumnVector) column).vector[row] = (rec.asLong != 0) ? 1 : 0;
        }
        if (values[offset].equalsIgnoreCase("true") || values[offset].equalsIgnoreCase("t")
            || values[offset].equals("1")) {
          ((LongColumnVector) column).vector[row] = 1;
        } else {
          ((LongColumnVector) column).vector[row] = 0;
        }
      }
    }
  }

  class LongConverter extends ConverterImpl {
    LongConverter(IntWritable offset) {
      super(offset);
    }

    @Override
    public void convert(String[] values, ColumnVector column, int row) {
      if (values[offset] == null || nullString.equals(values[offset])) {
        column.noNulls = false;
        column.isNull[row] = true;
      } else {
        ParseHexRec.ParsedRec rec = ParseHexRec.parse(values[offset]);
        if (rec != null && CsvReader.this.csvFormatBinary) {
          ((LongColumnVector) column).vector[row] = rec.asLong;
        } else {
          try {
            String val = null;
            try {
              val = CSV2ParquetTimestampUtils.parseDateOrIntStrict(values[offset]);
            } catch (NumberFormatException e) {

            }
            if (val == null) {
              try {
                val = Long.valueOf(CSV2ParquetTimestampUtils.parseTimeMicros(values[offset], true)).toString();
              } catch (ParseException e) {
              }
            }
            if (val == null) {
              try {
                val = Integer.valueOf(CSV2ParquetTimestampUtils.parseTimeMillisInt(values[offset], false)).toString();
              } catch (ParseException p) {
              }
            }
            if (val == null) {
              val = values[offset];
            }
            ((LongColumnVector) column).vector[row] = Long.parseLong(val);
          } catch (NumberFormatException ex) {
            System.err.println("Error in row:" + row + " column:" + offset + " expected parseable " + values[offset]);
            System.err.println(" type expected is : " + CsvReader.this.schema.getFieldNames().get(offset) + " "
                + CsvReader.this.schema.getChildren().get(offset).toString());
            throw ex;
          }
        }
      }
    }
  }

  class DateConverter extends ConverterImpl {
    DateConverter(IntWritable offset) {
      super(offset);
    }

    @Override
    public void convert(String[] values, ColumnVector column, int row) {
      if (values[offset] == null || nullString.equals(values[offset])) {
        column.noNulls = false;
        column.isNull[row] = true;
      } else {
        ParseHexRec.ParsedRec rec = ParseHexRec.parse(values[offset]);
        if (rec != null && CsvReader.this.csvFormatBinary) {
          ((LongColumnVector) column).vector[row] = rec.asLong;
        } else {
          String val = CSV2ParquetTimestampUtils.parseDateOrIntSloppy(values[offset]);
          ((LongColumnVector) column).vector[row] = Long.parseLong(val);
        }

      }
    }
  }

  class DoubleConverter extends ConverterImpl {
    DoubleConverter(IntWritable offset) {
      super(offset);
    }

    @Override
    public void convert(String[] values, ColumnVector column, int row) {
      if (values[offset] == null || nullString.equals(values[offset])) {
        column.noNulls = false;
        column.isNull[row] = true;
      } else {
        ParseHexRec.ParsedRec rec = ParseHexRec.parse(values[offset]);
        if (rec != null && CsvReader.this.csvFormatBinary) {
          if (rec.binary.length() == 4) {
            ((DoubleColumnVector) column).vector[row] = rec.asFloat;
          } else {
            ((DoubleColumnVector) column).vector[row] = rec.asDouble;
          }
        } else {
          ((DoubleColumnVector) column).vector[row] = Double.parseDouble(values[offset]);
        }
      }
    }
  }

  class DecimalConverter extends ConverterImpl {
    private final int scale;

    DecimalConverter(IntWritable offset, int scale) {
      super(offset);
      this.scale = scale;
    }

    @Override
    public void convert(String[] values, ColumnVector column, int row) {
      if (values[offset] == null || nullString.equals(values[offset])) {
        column.noNulls = false;
        column.isNull[row] = true;
      } else {
        ParseHexRec.ParsedRec rec = ParseHexRec.parse(values[offset]);
        if (rec != null && CsvReader.this.csvFormatBinary) {
          if (rec.binary.length() <= 8) {
            HiveDecimalWritable hdw = new HiveDecimalWritable();
            hdw.setFromLongAndScale(rec.asLong, scale);
            ((DecimalColumnVector) column).vector[row].set(hdw);
          } else {
            BigInteger bi = new BigInteger(rec.binary.getBytes());// (bytes, row, length); = rec.asDouble;
            HiveDecimalWritable hdw = new HiveDecimalWritable();
            hdw.setFromBigIntegerBytesAndScale(bi.toByteArray(), scale);
            ((DecimalColumnVector) column).vector[row].set(hdw);
          }
        } else {
          ((DecimalColumnVector) column).vector[row].set(new HiveDecimalWritable(values[offset]));
        }
      }
    }
  }

  class BytesConverter extends ConverterImpl {
    BytesConverter(IntWritable offset) {
      super(offset);
    }

    @Override
    public void convert(String[] values, ColumnVector column, int row) {
      if (values[offset] == null || nullString.equals(values[offset])) {
        column.noNulls = false;
        column.isNull[row] = true;
      } else {
        ParseHexRec.ParsedRec rec = ParseHexRec.parse(values[offset]);
        if (rec != null && CsvReader.this.csvFormatBinary) {
          byte[] value = rec.binary.getBytes();
          ((BytesColumnVector) column).setRef(row, value, 0, value.length);
        } else {
          byte[] value = values[offset].getBytes(StandardCharsets.UTF_8);
          ((BytesColumnVector) column).setRef(row, value, 0, value.length);
        }
      }
    }
  }

  class TimestampConverter extends ConverterImpl {
    TimestampConverter(IntWritable offset) {
      super(offset);
    }

    @Override
    public void convert(String[] values, ColumnVector column, int row) {
      if (values[offset] == null || nullString.equals(values[offset])) {
        column.noNulls = false;
        column.isNull[row] = true;
      } else {
        TimestampColumnVector vector = (TimestampColumnVector) column;

        ParseHexRec.ParsedRec rec = ParseHexRec.parse(values[offset]);
        if (rec != null && CsvReader.this.csvFormatBinary) {
          Timestamp timestamp = new Timestamp(rec.asLong);
          vector.set(row, timestamp);
        } else {
          TemporalAccessor temporalAccessor = null;
          try {
            temporalAccessor = DATE_TIME_FORMATTER.parseBest(values[offset], ZonedDateTime.FROM, LocalDateTime.FROM);
          } catch (DateTimeParseException ex) {
            try {
              long tm = CSV2ParquetTimestampUtils.parseTimeStampMicros(values[offset], false);
              Timestamp a = new Timestamp(tm / 1000);
              long remainder = (tm - ((long) (tm / 1000)) * 1000);
              while (remainder < 0) {
                remainder += 1000;
              }
              a.setNanos((int) remainder * 1000);
              vector.set(row, a);
            } catch (ParseException ex2) {
              try {
                long u = Long.parseLong(values[offset]);
                Timestamp a = new Timestamp(u);
                vector.set(row, a);
              } catch (NumberFormatException ex3) {
                System.err.println(
                    "Error in row:" + row + " column:" + offset + " expected Timestamp parseable " + values[offset]);
                System.err.println(" type expected is : " + CsvReader.this.schema.getFieldNames().get(offset) + " "
                    + CsvReader.this.schema.getChildren().get(offset).toString());
                String msg = "Error in row:" + row + " column:" + offset + " expected Timestamp parseable "
                    + values[offset] + " type expected is : " + CsvReader.this.schema.getFieldNames().get(offset) + " "
                    + CsvReader.this.schema.getChildren().get(offset).toString();
                throw new DateTimeParseException(msg, values[offset], ex2.getErrorOffset(), ex2);
              }
            }
          }
          if (temporalAccessor instanceof ZonedDateTime) {
            vector.set(row, new Timestamp(((ZonedDateTime) temporalAccessor).toEpochSecond() * 1000L));
          } else if (temporalAccessor instanceof LocalDateTime) {
            vector.set(row, new Timestamp(
                ((LocalDateTime) temporalAccessor).atZone(ZoneId.systemDefault()).toEpochSecond() * 1000L));
          } else {
            column.noNulls = false;
            column.isNull[row] = true;
          }
        }
      }
    }
  }

  class StructConverter implements Converter {
    final Converter[] children;

    StructConverter(IntWritable offset, TypeDescription schema) {
      children = new Converter[schema.getChildren().size()];
      int c = 0;
      for (TypeDescription child : schema.getChildren()) {
        children[c++] = buildConverter(offset, child);
      }
    }

    @Override
    public void convert(String[] values, VectorizedRowBatch batch, int row) {
      for (int c = 0; c < children.length; ++c) {
        children[c].convert(values, batch.cols[c], row);
      }
    }

    @Override
    public void convert(String[] values, ColumnVector column, int row) {
      StructColumnVector cv = (StructColumnVector) column;
      for (int c = 0; c < children.length; ++c) {
        children[c].convert(values, cv.fields[c], row);
      }
    }
  }

  Converter buildConverter(IntWritable startOffset, TypeDescription schema) {
    switch (schema.getCategory()) {
    case BOOLEAN:
      return new BooleanConverter(startOffset);
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      return new LongConverter(startOffset);
    case DATE:
      return new DateConverter(startOffset);
    case FLOAT:
    case DOUBLE:
      return new DoubleConverter(startOffset);
    case DECIMAL:
      return new DecimalConverter(startOffset, schema.getScale());
    case BINARY:
    case STRING:
    case CHAR:
    case VARCHAR:
      return new BytesConverter(startOffset);
    case TIMESTAMP:
      return new TimestampConverter(startOffset);
    case STRUCT:
      return new StructConverter(startOffset, schema);
    default:
      throw new IllegalArgumentException("Unhandled type " + schema);
    }
  }
}
