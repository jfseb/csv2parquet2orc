/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Minor modifications (c) Gerd Forstmann 2017
 */

package jfseb.csv2parquet;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.tools.FileDump;
import org.apache.parquet.tools.Main;

import jfseb.csv2parquet.convert.ConvertToolOrc;
import jfseb.csv2parquet.convert.ConvertToolParquet;
import jfseb.csv2parquet.convert.ConvertToolBase.Format;
import jfseb.csv2parquet.parquet.DumpMeta;
import jfseb.csv2parquet.parquet.DumpSchema;

/**
 * Driver program for the java ORC utilities.
 */
public class Driver {

  @SuppressWarnings("static-access")
  static Options createOptions() {
    Options result = new Options();

    result.addOption(OptionBuilder.withLongOpt("help").withDescription("Print help message").create('h'));

    result.addOption(OptionBuilder.withLongOpt("define").withDescription("Set a configuration property").hasArg()
        .withValueSeparator().create('D'));
    return result;
  }

  static class DriverOptions {
    final CommandLine genericOptions;
    final String command;
    final String[] commandArgs;

    DriverOptions(String[] args) throws ParseException {
      genericOptions = new GnuParser().parse(createOptions(), args, true);
      String[] unprocessed = genericOptions.getArgs();
      if (unprocessed.length == 0) {
        command = null;
        commandArgs = new String[0];
      } else {
        command = unprocessed[0];
        if (genericOptions.hasOption('h')) {
          commandArgs = new String[] { "-h" };
        } else {
          commandArgs = new String[unprocessed.length - 1];
          System.arraycopy(unprocessed, 1, commandArgs, 0, commandArgs.length);
        }
      }
    }
  }
  
  private final static String[] COMMANDS = { "data", "meta", "schema", "convert", "dump" };

  public static String getInput(String[] args) throws Exception {
    int i = 0;
    while (i < args.length && Arrays.asList(COMMANDS).indexOf(args[i]) < 0) {
      i = i + 1;

    }
    i = i + 1; // skip command;
    if (i == args.length)
      return null;
    // skip parameters -d <par>
    while (i < args.length && args[i].length() > 0 && args[i].charAt(0) == '-') {
      i = i + 1;
    }
    if (i == args.length)
      return null;
    return args[i];
  }

  public static boolean hasInputArgs(String[] args, String[] testedArg) throws Exception {
    int i = 0;
    List<String> lst = Arrays.asList(testedArg);
    while (i < args.length && Arrays.asList(COMMANDS).indexOf(args[i]) < 0) {
      i = i + 1;
    }
    i = i + 1; // skip command;
    if (i == args.length)
      return false;
    // skip parameters -d <par>
    while (i < args.length && args[i].length() > 0) {
      if (args[i].charAt(0) == '-') {

        if (lst.indexOf(args[i].substring(1)) >= 0) {
          return true;
        }
      }
      i = i + 1;
    }
    return false;
  }

  public static String getOutput(String[] args) throws Exception {
    for (int i = 0; i < args.length; ++i) {
      if ("-o".equals(args[i]) || "--output".equals(args[i]) && args.length > i + 1) {
        return args[i + 1];
      }
    }
    return null;
  }

  public static void main(String[] args) throws Exception {
    Locale.setDefault(Locale.US);
    
    if (Main.out == null) {
      Main.out = System.out; 
    }
    if (Main.err == null) {
      Main.err = System.err; 
    }
    
    DriverOptions options = new DriverOptions(args);

    if (options.command == null) {
      System.err.println("CSV to parquet converter (csv2parquet2orc)  parquet 1.9.0 orc 1.4.0 ");
      System.err.println();
      System.err.println("usage: java -jar csv2parquet*.jar [--help]" + " [--define X=Y] <command> <args>");
      System.err.println();
      System.err.println("Commands:");
      System.err.println("   meta - print the metadata about the ORC/Parquet file");
      System.err.println("   data - print the data from the ORC/Parquet file");
      System.err.println("   scan - scan the ORC/Parquet file");
      System.err.println("   convert - convert CSV to ORC/Parquet");
      System.err.println("   json-schema - scan JSON files to determine their schema");
      System.err.println();
      System.err.println(" examples: ");
      System.err.println(" java -jar target/csv2parquet2orc*-jar-with-dependencies.jar  -D csvformat=binary convert parquet-testdata/csvbinary/csvbinary.csv -s parquet-testdata/csvbinary/csvbinary.parquet.schema  -o out.parquet  -S\"|\" ");
      System.err.println(" java -jar target/csv2parquet2orc*-jar-with-dependencies.jar  meta out.parquet ");    
      System.err.println();
      System.err.println("To get more help, provide -h to the command");
      System.err.println();
      System.err.println("For other writer/reader versions, see");
      System.err.println(" - https://github.com/jfseb/csv2parquet2orc       (parquet  1.9.x ; orc 1.4.x)");
      System.err.println(" - https://github.com/jfseb/csv2parquet2orc_p1_10 (parquet 1.10.x ; orc 1.5.x)");      
 
      System.exit(1);
    }
    Configuration conf = new Configuration();
    Properties confSettings = options.genericOptions.getOptionProperties("D");
    for (Map.Entry pair : confSettings.entrySet()) {
      String arr[] = pair.getKey().toString().split("=");
      conf.set(arr[0], arr[1]);
      // pair.getKey().toString(), pair.getValue().toString());
    }
    if ("meta".equals(options.command)) {

      if (isOrc(getInput(args))) {
        FileDump.main(conf, options.commandArgs);
      } else {
        DumpMeta.execute(getInput(args));
      }
    } else if ("schema".equals(options.command)) {
      if (isOrc(getInput(args))) {
        
        String[] matches = { "x", "extended" };
        boolean detailed = hasInputArgs(args, matches);
        DumpSchema.execute(getInput(args), detailed, Format.ORC);
      } else {
        String[] matches = { "x", "extended" };
        boolean detailed = hasInputArgs(args, matches);
        DumpSchema.execute(getInput(args), detailed);
      }
    } else if ("data".equals(options.command)) {
      if (isOrc(getInput(args))) {
        PackageVisAdapter.PrintData_main(args);
      } else {
        throw new IllegalStateException("data not supported");
      }
      // FileDump.main(conf, options.commandArgs);
    } else if ("scan".equals(options.command)) {
      if (isOrc(getInput(args))) {
        PackageVisAdapter.ScanData_main(args);
        // ScanData.main(conf, options.commandArgs);
      } else {
        throw new IllegalStateException("scan not supported");
        // TODO
      }
    } else if ("json-schema".equals(options.command)) {
      // throw new IllegalStateException("json-schema not supported");
      org.apache.orc.tools.json.JsonSchemaFinder.main(conf, options.commandArgs);
    } else if ("convert".equals(options.command)) {
      // getInput(args)
      if (isOrc(getOutput(args))) {
        ConvertToolOrc.main(conf, options.commandArgs);
      } else {
        ConvertToolParquet.main(conf, options.commandArgs);
      }
    } else {
      System.err.println("Unknown subcommand: " + options.command);
      System.exit(1);
    }
  }

  private static boolean isOrc(String filename) {
    if (filename == null) {
      return false;
    }
    return "orc".equals(FilenameUtils.getExtension(filename).toLowerCase());
  }
}
