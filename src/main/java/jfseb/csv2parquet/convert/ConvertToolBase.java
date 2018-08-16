package jfseb.csv2parquet.convert;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import jfseb.prevorc15.org.apache.orc.tools.convert.CsvReader;

public class ConvertToolBase {

  protected final List<FileInformation> fileList;
  protected final String schemaString; // a schema string, if present, or null otherwise
  // protected final File schemaFile;
  public static class CSVOptions {
  public String csvSeparatorAsString;
  public char csvQuote;
  public char csvSeparatorAsChar;
  public char csvEscape;
  public int csvHeaderLines;
  public String csvNullString;
  public boolean csvFormatBinary;
  };
  protected CSVOptions csvOptions = new CSVOptions();
  protected final String outFileName;
  protected Configuration conf;
  protected CommandLine opts;
 
  protected enum Compression {
    NONE, GZIP
  }

  public enum Format {
    JSON, CSV, PARQUET, ORC, CSVTYPED
  }

  public static String toExtension(Format format) {
    switch (format) {
    case PARQUET:
      return ".parquet";
    case ORC:
      return ".orc";
    case CSV:
      return ".csv";
    default:
      throw new IllegalArgumentException(" not epecting " + format);
    }
  }

  protected class FileInformation {
    private final Compression compression;
    final Format format;
    private final Path path;
    private final FileSystem filesystem;
    private final Configuration conf;
    private final long size;

    FileInformation(Path path, Configuration conf) throws IOException {
      this.path = path;
      this.conf = conf;
      this.filesystem = path.getFileSystem(conf);
      this.size = getFilesystem().getFileStatus(path).getLen();
      String name = path.getName();
      int lastDot = name.lastIndexOf(".");
      if (lastDot >= 0 && ".gz".equals(name.substring(lastDot))) {
        this.compression = Compression.GZIP;
        name = name.substring(0, lastDot);
        lastDot = name.lastIndexOf(".");
      } else {
        this.compression = Compression.NONE;
      }
      if (lastDot >= 0) {
        String ext = name.substring(lastDot);
        if (".json".equals(ext) || ".jsn".equals(ext)) {
          format = Format.JSON;
        } else if (".csv".equals(ext)) {
          format = Format.CSV;
        } else if (".parquet".equals(ext)) {
          format = Format.PARQUET;
        } else if (".orc".equals(ext)) {
          format = Format.ORC;
        } else {
          throw new IllegalArgumentException("Unknown kind of file " + path);
        }
      } else {
        throw new IllegalArgumentException("No extension on file " + path);
      }
    }

    java.io.Reader getReader(InputStream input) throws IOException {
      if (compression == Compression.GZIP) {
        input = new GZIPInputStream(input);
      }
      return new InputStreamReader(input, StandardCharsets.UTF_8);
    }

    public RecordReader getRecordReader(TypeDescription schema) throws IOException {
      switch (format) {
      case ORC: {
        Reader reader = OrcFile.createReader(getPath(), OrcFile.readerOptions(conf));
        return reader.rows(reader.options().schema(schema));
      }
      case JSON: {
        FSDataInputStream underlying = getFilesystem().open(getPath());
        return new jfseb.prevorc15.org.apache.orc.tools.convert.JsonReader(getReader(underlying), underlying, size,
            schema);
      }
      case CSV: {
        FSDataInputStream underlying = getFilesystem().open(getPath());
        return new CsvReader(getReader(underlying), underlying, size, schema, csvOptions);
      }
      default:
        throw new IllegalArgumentException("Unhandled format " + format + " for " + getPath());
      }
    }

    public FileSystem getFilesystem() {
      return filesystem;
    }

    public Path getPath() {
      return path;
    }
  }

  protected static String getStringOption(CommandLine opts, char letter, String mydefault) {
    if (opts.hasOption(letter)) {
      return opts.getOptionValue(letter);
    } else {
      return mydefault;
    }
  }

  protected List<FileInformation> buildFileList(String[] files, Configuration conf) throws IOException {
    List<FileInformation> result = new ArrayList<FileInformation>(files.length);
    for (String fn : files) {
      result.add(new FileInformation(new Path(fn), conf));
    }
    return result;
  }

  protected static int getIntOption(CommandLine opts, char letter, int mydefault) {
    if (opts.hasOption(letter)) {
      return Integer.parseInt(opts.getOptionValue(letter));
    } else {
      return mydefault;
    }
  }

  protected static char getCharOption(CommandLine opts, char letter, char mydefault) {
    if (opts.hasOption(letter)) {
      return opts.getOptionValue(letter).charAt(0);
    } else {
      return mydefault;
    }
  }

  protected static CommandLine parseOptions(String[] args) throws ParseException {
    Options options = new Options();

    options.addOption(Option.builder("h").longOpt("help").desc("Provide help").build());
    options.addOption(Option.builder("s").longOpt("schema").hasArg()
        .desc("The schema filename to use to write in to the file, defaults to input.csv -> input.schema").build());
    options.addOption(Option.builder("o").longOpt("output")
        .desc(
            "Output filename, defaults to output.parquet \n options are: \n -D parquet.compression=[ZLIB,SNAPPY,NONE]\n"
                + "  -D parquet.BLOCK_SIZE=<int>  Default:["
                + String.valueOf(org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE) + "]\n"
                + "   -D parquet.PAGE_SIZEE=<int>  Default:["
                + String.valueOf(org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE) + "]\n" + "" 
                + "\n use -q -n -e -S -H to control quote/null/escape/separator/header respectively"
                + "\n use --csvformat binary to load binary values (or -D csvformat=binary \n"
                + "orc options are:\n -D orc.compress=[ZIP,SNAPPY,NONE]\n"
                + "  -D orc.stripe.size,  orc.compress.size, orc.row.index.stride, orc.create.index \n")
        .hasArg().build());

    options.addOption(Option.builder("q").longOpt("quote").desc("CSV quote character").hasArg().build());
    options.addOption(Option.builder("n").longOpt("null").desc("CSV null string").hasArg().build());
    options.addOption(Option.builder("e").longOpt("escape").desc("CSV escape character").hasArg().build());
    options.addOption(Option.builder("S").longOpt("separator").desc("CSV separator character").hasArg().build());
    options.addOption(Option.builder("H").longOpt("header").desc("CSV header lines").hasArg().build());
    options.addOption(Option.builder("f").longOpt("csvformat").desc("CSV format [default|binary]").hasArg().build());

    /*
     * options.addOption(Option.builder("n").longOpt("null").
     * desc("CSV null string, ignored!").hasArg().build());
     * options.addOption(Option.builder("q").longOpt("quote").
     * desc("CSV quote character, ignored!").hasArg().build());
     * options.addOption(Option.builder("e").longOpt("escape").
     * desc("CSV escape character, ignored!").hasArg().build());
     * options.addOption(Option.builder("S").longOpt("separator").
     * desc("CSV separator character").hasArg().build());
     * options.addOption(Option.builder("H").longOpt("header").
     * desc("CSV header lines, ignored!").hasArg().build());
     */
    CommandLine cli = new DefaultParser().parse(options, args);
    if (cli.hasOption('h') || cli.getArgs().length == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("convert", options);
      System.exit(1);
    }
    return cli;
  }

  public ConvertToolBase(Configuration conf, String[] args, Format target) throws IOException, ParseException {
    this.opts = parseOptions(args);
    this.conf = conf;
    this.fileList = buildFileList(opts.getArgs(), conf);
    this.schemaString = getSchemaString(target);
    this.csvOptions.csvQuote = getCharOption(opts, 'q', '"');
    this.csvOptions.csvEscape = getCharOption(opts, 'e', '\\');
    this.csvOptions.csvSeparatorAsString = getStringOption(opts, 'S', ",");
    this.csvOptions.csvSeparatorAsChar = this.csvOptions.csvSeparatorAsString.charAt(0);
    this.csvOptions.csvHeaderLines = getIntOption(opts, 'H', 0);
    this.csvOptions.csvFormatBinary = getStringOption(opts,'f', "default").toLowerCase().equals("binary");
    this.csvOptions.csvNullString = opts.getOptionValue('n', "");
    String filename = getDefaultOutFileName(target);
    this.outFileName = opts.hasOption('o') ? opts.getOptionValue('o') : filename;
    // writer = OrcFile.createWriter(new Path(outFilename),
    // OrcFile.writerOptions(conf).setSchema(schema));
    // batch = schema.createRowBatch();
  }

  protected static String stripExtensionDoubleForGZ(String filename) {
    if("gz".equals(FilenameUtils.getExtension(filename))) {
      filename = FilenameUtils.getPath(filename) + "/" +  FilenameUtils.getBaseName(filename); 
    }
    return FilenameUtils.getPath(filename) + "/" + FilenameUtils.getBaseName(filename);
  }
  
  public static String getDefaultSchemaByFile(FileInformation fileInformation, Format format) throws IOException {
    return getDefaultSchemaByFile(new File(fileInformation.getPath().toString()), format);
  }
    
  public static String getDefaultSchemaByFile(File csvFile, Format format) throws IOException {    
    String filename = stripExtensionDoubleForGZ(csvFile.toString());
    String fileName = filename + "." + getDefaultFormatName(format) +  ".schema";
    if (new File(fileName).exists()) {
      return ConvertUtils.readFile(fileName);
    }
    String filename2 = filename + ".schema";
    if (new File(filename2).exists()) {
      return ConvertUtils.readFile(filename2);
    }
    return null;
  }



  private String getSchemaString(Format format) throws IOException {
    String schemastr = null;
    if (opts.hasOption('s')) {
      String optValue = opts.getOptionValue('s');
      if (optValue.startsWith("struct<")) {
        return optValue;
      } else {
        return ConvertUtils.readFile(optValue);
      }
    } else {
      // derive schema from file
      return getDefaultSchemaByFile(fileList.get(0), format);
    }
  }

  public static String getDefaultFormatName(Format format) {
    if (format == Format.PARQUET) {
      return "parquet";
    }
    if (format == Format.ORC) {
      return "orc";
    }
    throw new IllegalArgumentException("no default infix for " + format);
  }
  
  private String getDefaultOutFileName(Format target) {
    String filename = "output.unkown";
    if (target == Format.PARQUET) {
      filename = "output.parquet";
    }
    if (target == Format.ORC) {
      filename = "output.orc";
    }
    return filename;
  }

}
