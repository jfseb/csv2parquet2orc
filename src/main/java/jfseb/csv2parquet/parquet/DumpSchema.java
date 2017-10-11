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

import java.io.IOException;

/* 
 * Modified variant of 
 https://github.com/apache/parquet-mr/blob/master/parquet-tools/src/main/java/org/apache/parquet/tools/command/ShowSchemaCommand.java
*/

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.TypeDescription.Category;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.tools.Main;

import jfseb.csv2parquet.TypeMapping;
import jfseb.csv2parquet.convert.ConvertToolBase.Format;

public class DumpSchema {

  public static void execute(String input, boolean detailed) throws Exception {

    Configuration conf = new Configuration();
    ParquetMetadata metaData;

    Path path = new Path(input);
    FileSystem fs = path.getFileSystem(conf);
    Path file;
    if (fs.isDirectory(path)) {
      FileStatus[] statuses = fs.listStatus(path, HiddenFileFilter.INSTANCE);
      if (statuses.length == 0) {
        throw new RuntimeException("Directory " + path.toString() + " is empty");
      }
      file = statuses[0].getPath();
    } else {
      file = path;
    }
    metaData = ParquetFileReader.readFooter(conf, file, ParquetMetadataConverter.NO_FILTER);
    MessageType schema = metaData.getFileMetaData().getSchema();

    Main.out.println(schema);
    if (detailed) {
      // PrettyPrintWriter out = PrettyPrintWriter.stdoutPrettyPrinter().build();
      // MetadataUtils.showDetails(out, metaData);
      // print SQL schema
      Main.out.println("");
      for (Iterator<Type> i = schema.getFields().listIterator(); i.hasNext();) {
        Type tp = i.next();
        String sqlType = TypeMapping.getSqlType(tp);
        String auxType = TypeMapping.getAuxSQLType(tp);
        boolean Null = TypeMapping.getNull(tp);
        Main.out.println(tp.getName() + " " + TypeMapping.getSqlType(tp) + (Null ? " NULL" : "") + ";"
            + ((auxType != null) ? "-- " + auxType : ""));
      }
      // print orc schema
    }
  }

  public static void execute(String input, boolean detailed, Format orc) throws IllegalArgumentException, IOException {
    Configuration conf = new Configuration();
    Reader reader = OrcFile.createReader(new Path(input), OrcFile.readerOptions(conf));

    TypeDescription schema = reader.getSchema();

    Main.out.println(schema.toString());

    if (detailed) {
      Main.out.println("\n");

      Main.out.println(schema.toJson());
      Main.out.println("\n");
      assert (schema.getCategory() == Category.STRUCT);
      List<String> fn = schema.getFieldNames();

      int k = 0;
      for (Iterator<TypeDescription> i = schema.getChildren().listIterator(); i.hasNext();) {
        TypeDescription tp = i.next();
        String name = fn.get(k);
        ++k;
        String sqlType = TypeMapping.getSqlType(tp);
        String auxType = TypeMapping.getAuxSQLType(tp);
        boolean Null = TypeMapping.getNull(tp);
        Main.out.println(name + " " + TypeMapping.getSqlType(tp) + (Null ? " NULL" : "") + ";"
            + ((auxType != null) ? "-- " + auxType : ""));
      }

    }
  }
}
