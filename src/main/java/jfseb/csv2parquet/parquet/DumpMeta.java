package jfseb.csv2parquet.parquet;
/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.PrintStream;

/* 
 * Modified variant of 
 https://github.com/apache/parquet-mr/blob/master/parquet-tools/src/main/java/org/apache/parquet/tools/command/ShowSchemaCommand.java
*/

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.tools.Main;
import org.apache.parquet.tools.util.MetadataUtils;
import org.apache.parquet.tools.util.PrettyPrintWriter;
import org.apache.parquet.tools.util.PrettyPrintWriter.Builder;
import org.apache.parquet.tools.util.PrettyPrintWriter.WhiteSpaceHandler;

public class DumpMeta {

  public static void execute(String input) throws Exception {    
    Configuration conf = new Configuration();
    Path inputPath = new Path(input);
    FileStatus inputFileStatus = inputPath.getFileSystem(conf).getFileStatus(inputPath);
    List<Footer> footers = ParquetFileReader.readFooters(conf, inputFileStatus, false);

    PrettyPrintWriter out = PrettyPrintWriter.stdoutPrettyPrinter()
                                             .withAutoColumn()
                                             .withWhitespaceHandler(WhiteSpaceHandler.COLLAPSE_WHITESPACE)
                                             .withColumnPadding(1)
                                             .build();
   
    for(Footer f: footers) {
      out.format("file: %s%n" , f.getFile());
      MetadataUtils.showDetails(out, f.getParquetMetadata());
      out.flushColumns();
    }
  }
}
