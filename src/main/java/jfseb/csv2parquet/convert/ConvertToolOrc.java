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

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

/**
 * A conversion tool to convert CSV or JSON files into ORC files.
 */
public class ConvertToolOrc extends ConvertToolBase {

  private final TypeDescription schema;
  private final Writer writer;
  private final VectorizedRowBatch batch;

  TypeDescription buildSchema(List<FileInformation> files, Configuration conf) throws IOException {
    jfseb.prevorc15.org.apache.orc.tools.json.JsonSchemaFinder schemaFinder = new jfseb.prevorc15.org.apache.orc.tools.json.JsonSchemaFinder();
    int filesScanned = 0;
    for (FileInformation file : files) {
      if (file.format == Format.JSON) {
        System.err.println("Scanning " + file.getPath() + " for schema");
        filesScanned += 1;
        schemaFinder.addFile(file.getReader(file.getFilesystem().open(file.getPath())));
      } else if (file.format == Format.ORC) {
        System.err.println("Merging schema from " + file.getPath());
        filesScanned += 1;
        Reader reader = OrcFile.createReader(file.getPath(),
            OrcFile.readerOptions(conf).filesystem(file.getFilesystem()));
        schemaFinder.addSchema(reader.getSchema());
      }
    }
    if (filesScanned == 0) {
      // take first input, calculate 
      String schemaStr = getDefaultSchemaByFile(files.get(0),Format.ORC);
      if( schemaStr == null) {
        throw new IllegalArgumentException("Please specify a schema using" + " --schema for converting CSV files.");      
      }
      // cleanse all string. 
      schemaStr = schemaStr.replaceAll("\\n", "");
      schemaStr = schemaStr.replaceAll("\\r", "");
      schemaStr = schemaStr.replaceAll("\\s","");
      return TypeDescription.fromString(schemaStr);
    }
    return schemaFinder.getSchema();
  }



  public static void main(Configuration conf, String[] args) throws IOException, ParseException {
    new ConvertToolOrc(conf, args).run();
  }


  /*
   * List<FileInformation> buildFileList(String[] files, Configuration conf)
   * throws IOException { List<FileInformation> result = new
   * ArrayList<>(files.length); for(String fn: files) { result.add(new
   * FileInformation(new Path(fn), conf)); } return result; }
   */

  public ConvertToolOrc(Configuration conf, String[] args) throws IOException, ParseException {
    super(conf, args, Format.ORC);
    /*
     * CommandLine opts = parseOptions(args); fileList =
     * buildFileList(opts.getArgs(), conf);
     */

    if (opts.hasOption('s')) {
      String schemaStr = this.schemaString.replaceAll("\\n", "");
      schemaStr = schemaStr.replaceAll("\\r", "");
      schemaStr = schemaStr.replaceAll("\\s","");
      this.schema = TypeDescription.fromString(schemaStr);
    } else {
      this.schema = buildSchema(this.fileList, conf);
    }

    /*
     * this.csvQuote = getCharOption(opts, 'q', '"'); this.csvEscape =
     * getCharOption(opts, 'e', '\\'); this.csvSeparator = getCharOption(opts, 'S',
     * ','); this.csvHeaderLines = getIntOption(opts, 'H', 0); this.csvNullString =
     * opts.getOptionValue('n', "");
     */
    String outFilename = opts.hasOption('o') ? opts.getOptionValue('o') : "output.orc";
    writer = OrcFile.createWriter(new Path(outFilename), OrcFile.writerOptions(conf).setSchema(schema));
    batch = schema.createRowBatch();
  }

  void run() throws IOException {
    for (FileInformation file : fileList) {
      System.err.println("Processing " + file.getPath());
      RecordReader reader = file.getRecordReader(this.schema);
      while (reader.nextBatch(batch)) {
        writer.addRowBatch(batch);
      }
      reader.close();
    }
    writer.close();
  }

  /*
   * private static CommandLine parseOptions(String[] args) throws ParseException
   * { Options options = new Options();
   * 
   * options.addOption(Option.builder("h").longOpt("help").desc("Provide help").
   * build()); options .addOption(Option.builder("s").longOpt("schema").hasArg().
   * desc("The schema to write in to the file").build());
   * options.addOption(Option.builder("o").longOpt("output").
   * desc("Output filename").hasArg().build());
   * options.addOption(Option.builder("n").longOpt("null").desc("CSV null string")
   * .hasArg().build()); options.addOption(Option.builder("q").longOpt("quote").
   * desc("CSV quote character").hasArg().build());
   * options.addOption(Option.builder("e").longOpt("escape").
   * desc("CSV escape character").hasArg().build());
   * options.addOption(Option.builder("S").longOpt("separator").
   * desc("CSV separator character").hasArg().build());
   * options.addOption(Option.builder("H").longOpt("header").
   * desc("CSV header lines").hasArg().build()); CommandLine cli = new
   * DefaultParser().parse(options, args); if (cli.hasOption('h') ||
   * cli.getArgs().length == 0) { HelpFormatter formatter = new HelpFormatter();
   * formatter.printHelp("convert", options); System.exit(1); } return cli; }
   */
}
