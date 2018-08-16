package jfseb.csv2parquet.convert;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Log;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import jfseb.csv2parquet.Utils;
import jfseb.csv2parquet.convert.ConvertToolBase.CSVOptions;
import jfseb.csv2parquet.convert.utils.CSV2ParquetTimestampUtils;
import jfseb.csv2parquet.parquet.CsvParquetWriter;
import jfseb.csv2parquet.utils.SchemaCreator;
import jfseb.csv2parquet.utils.USchema;

public class ConvertUtils {

  private static final Log LOG = Log.getLog(ConvertUtils.class);

  public static final String DEFAULT_CSV_DELIMITER = "|";
  // public String CSV_DELIMITER= "|";

  private static String CSV_DELIMITER = DEFAULT_CSV_DELIMITER;

  public static String readFile(String path) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(path));
    StringBuilder stringBuilder = new StringBuilder();

    try {
      String line = null;
      String ls = System.getProperty("line.separator");

      while ((line = reader.readLine()) != null) {
        stringBuilder.append(line);
        stringBuilder.append(ls);
      }
    } finally {
      Utils.closeQuietly(reader);
    }

    return stringBuilder.toString();
  }

  public static String getSchema(File schemaFile) throws IOException {
    return readFile(schemaFile.getAbsolutePath());
  }

  public static void convertCsvToParquet(File csvFile, File outputParquetFile) throws IOException {
    convertCsvToParquet(csvFile, outputParquetFile, false);
  }

  public static void convertCsvToParquet(File csvFile, File outputParquetFile, boolean enableDictionary)
      throws IOException {
    String schemaString = ConvertToolBase.getDefaultSchemaByFile(csvFile, ConvertToolBase.Format.PARQUET);

    CSVOptions csvOptions = new CSVOptions();
    csvOptions.csvSeparatorAsString = ConvertUtils.DEFAULT_CSV_DELIMITER;
    convertCsvToParquet(csvFile, outputParquetFile, schemaString, enableDictionary, csvOptions,
        null);
  }

  static java.io.Reader getReader(InputStream input, File filename) throws IOException {
    if (filename.toString().endsWith((".gz"))) {
      input = new GZIPInputStream(input);
    }
    return new InputStreamReader(input, StandardCharsets.UTF_8);
  }

  public static void convertCsvToParquet(File csvFile, File outputParquetFile, String schemaString,
      boolean enableDictionary, CSVOptions csvOptions, Configuration conf) throws IOException {

    ConvertUtils.CSV_DELIMITER = csvOptions.csvSeparatorAsString;
    System.setProperty("line.separator", "\n");
    LOG.info("Converting " + csvFile.getName() + " to " + outputParquetFile.getName());
    if(schemaString.indexOf("\r\n") > 0 ) {
      schemaString = schemaString.replaceAll("\\r\\n", "\n");
    }
    String rawSchema = schemaString;
    if (outputParquetFile.exists()) {
      throw new IOException("Output file " + outputParquetFile.getAbsolutePath() + " already exists");
    }

    Path path = new Path(outputParquetFile.toURI());
    CompressionCodecName codecName = CompressionCodecName.UNCOMPRESSED;
    int block_size = org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
    int page_size = org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;
    boolean  readAsBinary = false;
    if (conf != null) {
      String inputbin = conf.get("csvformat","default");
      if( "binary".equals(inputbin) || "BINARY".equals(inputbin)) {
        readAsBinary = true;
      }
      String cmdline = conf.get("parquet.compress", "GZIP");
      if ("ZIP".equals(cmdline) || "GZIP".equals(cmdline)) {
        codecName = CompressionCodecName.GZIP;
      } else if ("SNAPPY".equals(cmdline)) {
      } else if ("NONE".equals(cmdline) || "UNCOMPRESSED".equals(cmdline)) {
        codecName = CompressionCodecName.UNCOMPRESSED;
      } else {
        throw new IllegalArgumentException(" parquet.compress must be [ZIP=GZIP, SNAPPY, NONE=UNCOMPRESSED]");
      }
      block_size = conf.getInt("parquet.BLOCK_SIZE", org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE);
      page_size = conf.getInt("parquet.PAGE_SIZE", org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE);
    }
    readAsBinary |= csvOptions.csvFormatBinary;

    USchema schemas = SchemaCreator.makeSchema(csvFile,schemaString);
    
    MessageType schema = MessageTypeParser.parseMessageType(rawSchema);
    CsvParquetWriter writer = new CsvParquetWriter(path, schema, codecName, block_size, page_size, enableDictionary, readAsBinary);

    BufferedReader br = new BufferedReader(getReader(new FileInputStream(csvFile), csvFile));
    String line;
    int lineNumber = 0;
    try {
      while ((line = br.readLine()) != null) {
        if( lineNumber >= csvOptions.csvHeaderLines) {
          String[] fields = line.split(Pattern.quote(CSV_DELIMITER));
  
          int cols = schema.getColumns().size();
          ArrayList<String> lst = new ArrayList<String>(Arrays.asList(fields)); // Arrays.asList(fields);
          while (lst.size() < cols) {
            lst.add("");
          }
          while (lst.size() > cols) {
            lst.remove(lst.size() - 1);
          }
          writer.write(lst);
        }
        ++lineNumber;        
      }

      writer.close();
    } catch (java.lang.NumberFormatException e) {
      LOG.error("error" + e.toString() + " " + lineNumber);
      throw new IllegalArgumentException(">>line number : " + lineNumber, e);
    } finally {
      LOG.info("Number of lines: " + lineNumber);
      Utils.closeQuietly(br);
    }
  }

  public static void convertParquetToCSV(File parquetFile, File csvOutputFile) throws IOException {
    convertParquetToCSV(parquetFile, csvOutputFile, ConvertUtils.DEFAULT_CSV_DELIMITER);
  }

  public static void convertParquetToCSV(File parquetFile, File csvOutputFile, String csvDelimiter) throws IOException {
    Preconditions.checkArgument(parquetFile.getName().endsWith(".parquet"),
        "parquet file should have .parquet extension");
    Preconditions.checkArgument(csvOutputFile.getName().endsWith(".csv"), "csv file should have .csv extension");
    Preconditions.checkArgument(!csvOutputFile.exists(),
        "Output file " + csvOutputFile.getAbsolutePath() + " already exists");

    LOG.info("Converting " + parquetFile.getName() + " to " + csvOutputFile.getName());

    Path parquetFilePath = new Path(parquetFile.toURI());

    Configuration configuration = new Configuration(true);

    GroupReadSupport readSupport = new GroupReadSupport();
    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, parquetFilePath);
    MessageType schema = readFooter.getFileMetaData().getSchema();

    readSupport.init(configuration, null, schema);
    BufferedWriter w = new BufferedWriter(new FileWriter(csvOutputFile));
    ParquetReader<Group> reader = new ParquetReader<Group>(parquetFilePath, readSupport);
    try {
      Group g = null;
      while ((g = reader.read()) != null) {
        writeGroup(w, g, schema, csvDelimiter);
      }
      reader.close();
    } finally {
      Utils.closeQuietly(w);
    }
  }

  private static void writeGroup(BufferedWriter w, Group g, MessageType schema, String csvDelimiter)
      throws IOException {
    for (int j = 0; j < schema.getFieldCount(); j++) {
      if (j > 0) {
        w.write(csvDelimiter);
      }
      if(schema.getType(j).asPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.INT96) {
        String valueString = CSV2ParquetTimestampUtils.binaryToDateTimeString(g.getInt96(j,0));
        w.write(valueString);
      } else if(schema.getType(j).asPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.INT32
          && schema.getType(j).getOriginalType() == OriginalType.TIME_MILLIS) {
        String valueString = CSV2ParquetTimestampUtils.formatTimeMillis(g.getInteger(j,0));
        w.write(valueString);
      } else if(schema.getType(j).asPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.INT64
          && schema.getType(j).getOriginalType() == OriginalType.TIME_MICROS) {
        String valueString = CSV2ParquetTimestampUtils.formatTimeMicros(g.getLong(j,0));
        w.write(valueString);
      } else if(schema.getType(j).asPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.INT32
          && schema.getType(j).getOriginalType() == OriginalType.DATE) {
        String valueString = CSV2ParquetTimestampUtils.formatDate(g.getInteger(j,0));
        w.write(valueString);
      } else {
        String valueToString = g.getValueToString(j, 0);
        w.write(valueToString);
      }
    }
    w.write('\n');
  }

  @Deprecated
  public static void convertParquetToCSVEx(File parquetFile, File csvOutputFile) throws IOException {
    Preconditions.checkArgument(parquetFile.getName().endsWith(".parquet"),
        "parquet file should have .parquet extension");
    Preconditions.checkArgument(csvOutputFile.getName().endsWith(".csv"), "csv file should have .csv extension");
    Preconditions.checkArgument(!csvOutputFile.exists(),
        "Output file " + csvOutputFile.getAbsolutePath() + " already exists");

    LOG.info("Converting " + parquetFile.getName() + " to " + csvOutputFile.getName());

    Path parquetFilePath = new Path(parquetFile.toURI());

    Configuration configuration = new Configuration(true);

    // TODO Following can be changed by using ParquetReader instead of
    // ParquetFileReader
    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, parquetFilePath);
    MessageType schema = readFooter.getFileMetaData().getSchema();
    ParquetFileReader parquetFileReader = new ParquetFileReader(configuration, parquetFilePath, readFooter.getBlocks(),
        schema.getColumns());
    BufferedWriter w = new BufferedWriter(new FileWriter(csvOutputFile));
    PageReadStore pages = null;
    try {
      while (null != (pages = parquetFileReader.readNextRowGroup())) {
        final long rows = pages.getRowCount();
        LOG.info("Number of rows: " + rows);

        final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
        final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
        for (int i = 0; i < rows; i++) {
          final Group g = recordReader.read();
          writeGroup(w, g, schema, ConvertUtils.DEFAULT_CSV_DELIMITER);
        }
      }
    } finally {
      Utils.closeQuietly(parquetFileReader);
      Utils.closeQuietly(w);
    }
  }

}