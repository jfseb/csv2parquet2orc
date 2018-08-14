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

package jfseb.csv2parquet.parquet;

import java.io.File;
import java.io.IOException;

import org.apache.parquet.Log;
import org.junit.Test;

import jfseb.csv2parquet.Driver;
import jfseb.csv2parquet.Utils;
import jfseb.csv2parquet.convert.ConvertToolBase;
import jfseb.csv2parquet.convert.ConvertUtils;
import junit.framework.Assert;

/**
 * This tests compatibility of parquet format (written by java code) from older
 * versions of parquet with the current version.
 * 
 * Parquet files for previous versions are assumed to be generated under under
 * $PROJECT_HOME/parquet-compat-$version/target/parquet/ If files are not
 * present, a WARNing is generated.
 * 
 * @author amokashi
 *
 */
public class TestBackwardsCompatibility {

  private static final Log LOG = Log.getLog(TestBackwardsCompatibility.class);

  @Test
  public void testReadWriteCompatibility() throws IOException {
    File[] csvFiles = Utils.getAllOriginalCSVFiles();
    for (File csvFile : csvFiles) {
      String filename = Utils.getFileNamePrefix(csvFile);

      // With no dictionary - default
      File parquetTestFile = Utils.getParquetOutputFile(filename, "plain", true);
      ConvertUtils.convertCsvToParquet(csvFile, parquetTestFile);
      File csvTestFile = Utils.getCsvTestFile(filename, "plain", true);
      ConvertUtils.convertParquetToCSV(parquetTestFile, csvTestFile);

      Utils.verify(csvFile, csvTestFile);

      // With dictionary encoding
      parquetTestFile = Utils.getParquetOutputFile(filename, "dict", true);
      ConvertUtils.convertCsvToParquet(csvFile, parquetTestFile, true);
      csvTestFile = Utils.getCsvTestFile(filename, "dict", true);
      ConvertUtils.convertParquetToCSV(parquetTestFile, csvTestFile);

      Utils.verify(csvFile, csvTestFile);
    }
  }

  @Test
  public void testReadWriteTypesCompatibility() throws Exception {
    File[] csvFiles = Utils.getAllManyTypesCSVFiles();
    for (File csvFile : csvFiles) {
      String filename = Utils.getFileNamePrefix(csvFile);

      // With no dictionary - default
      File parquetTestFile = Utils.getParquetOutputFile(filename, "plain", true);
      ConvertUtils.convertCsvToParquet(csvFile, parquetTestFile);
      File csvTestFile = Utils.getCsvTestFile(filename, "plain", true);
      ConvertUtils.convertParquetToCSV(parquetTestFile, csvTestFile);

      final File canonicName = new File(csvFile + ".canonic");
      if( canonicName.exists()) {
        Utils.verify(canonicName, csvTestFile);
      } else {
        Utils.verify(csvFile, csvTestFile);
      }
      
      // With dictionary encoding
      parquetTestFile = Utils.getParquetOutputFile(filename, "dict", true);
      ConvertUtils.convertCsvToParquet(csvFile, parquetTestFile, true);
      csvTestFile = Utils.getCsvTestFile(filename, "dict", true);
      ConvertUtils.convertParquetToCSV(parquetTestFile, csvTestFile);
      
      if( canonicName.exists()) {
        Utils.verify(canonicName, csvTestFile);
      } else {
        Utils.verify(csvFile, csvTestFile);
      }
      // types
      String[] compatibleVersions = Utils.getAllPreviousVersionDirs();
      String version = "1.9.0"; // compatibleVersions[0];
      String prefix = Utils.getFileNamePrefix(csvFile);
      File versionOrcFile = Utils.getRefFile(prefix, version, "plain", true, ConvertToolBase.Format.PARQUET, null);

      File testMeta = Utils.getMetaTestFile(prefix, version, true, ConvertToolBase.Format.PARQUET);
      File refMeta = Utils.getMetaRefFile(prefix, version,null,  ConvertToolBase.Format.PARQUET);

      StringRedirector redir = null;
      try {
        redir = new StringRedirector(StringRedirector.outputStream(testMeta));
        String[] args = { "meta", versionOrcFile.toString() };
        Driver.main(args);
      } catch (Exception e) {
        e.printStackTrace();
        System.err.println(e);
        Assert.fail("exception" + e);
        throw e;
      } finally {
        redir.restore();
      }
      String[][] canonize = { { "^file:.*", "file:<afile>" }, { "PLAIN,BIT_PACKED", "BIT_PACKED,PLAIN" },
          { "RLE,PLAIN", "PLAIN,RLE" }, { "RLE,BIT_PACKED", "BIT_PACKED,RLE" },
          { "PLAIN,BIT_PACKED", "BIT_PACKED,PLAIN"},
          { "RLE,PLAIN", "PLAIN,RLE"}};
      Utils.verifyCanonize(refMeta, testMeta, canonize);
    }
  }


  @Test
  public void testReadCSVBinaryCompatibility() throws Exception {
    File[] csvFiles = Utils.getAllBinaryCSVFiles();
    for (File csvFile : csvFiles) {
      String filename = Utils.getFileNamePrefix(csvFile);

      // With no dictionary - default
      File parquetTestFile = Utils.getParquetOutputFile(filename, "plain", true);
      //ConvertUtils.convertCsvToParquet(csvFile, parquetTestFile);
      File csvTestFile = Utils.getCsvTestFile(filename, "plain", true);
      //ConvertUtils.convertParquetToCSV(parquetTestFile, csvTestFile);

      final File canonicName = new File(csvFile + ".canonic");
     /*
      if( canonicName.exists()) {
        Utils.verify(canonicName, csvTestFile);
      } else {
        Utils.verify(csvFile, csvTestFile);
      }
     */ 
      // from binary csv encoding
      parquetTestFile = Utils.getParquetOutputFile(filename, "binary", true);
      //ConvertUtils.convertCsvToParquet(csvFile, parquetTestFile, true);
     
      csvTestFile = Utils.getCsvTestFile(filename, "binary", true);
      //ConvertUtils.convertParquetToCSV(parquetTestFile, csvTestFile);
      
      if( canonicName.exists()) {
        // Utils.verify(canonicName, csvTestFile);
      } else {
        // Utils.verify(csvFile, csvTestFile);
      }
      // types
      String[] compatibleVersions = Utils.getAllPreviousVersionDirs();
      String version = "1.9.0"; // compatibleVersions[0];
      String prefix = Utils.getFileNamePrefix(csvFile);
      //File versionOrcFile = Utils.getRefFile(prefix, version, "plain", true, ConvertToolBase.Format.PARQUET, null);

      File testMeta = Utils.getMetaTestFile(prefix, version, true, ConvertToolBase.Format.PARQUET);
      //File refMeta = Utils.getMetaRefFile(prefix, version,null,  ConvertToolBase.Format.PARQUET);

      StringRedirector redir = null;
      try {
        redir = new StringRedirector(StringRedirector.outputStream(testMeta));
        String[] args = {  "-Dcsvformat=binary", "convert",  csvFile.toString(),
            "-S" , "|", "-o" ,  parquetTestFile.toString()  };
        Driver.main(args);
      } catch (Exception e) {
        e.printStackTrace();
        System.err.println(e);
        Assert.fail("exception" + e);
        throw e;
      } finally {
        redir.restore();
      }
      csvTestFile = Utils.getCsvTestFile(filename, "binary", true);
      ConvertUtils.convertParquetToCSV(parquetTestFile, csvTestFile);
      if( canonicName.exists()) {
        Utils.verify(canonicName, csvTestFile);
      } else {
        Utils.verify(csvFile, csvTestFile);
      }      
    }
  }

  
  
  @Test
  public void testParquetBackwardsCompatibility() throws IOException {
    // read all versions of parquet files and convert them into csv
    // diff the csvs with original csvs
    File[] originalCsvFiles = Utils.getAllOriginalCSVFiles();
    String[] compatibleVersions = Utils.getAllPreviousVersionDirs();

    for (String version : compatibleVersions) {
      LOG.info("Testing compatibility with " + version);
      for (File originalCsvFile : originalCsvFiles) {

        String prefix = Utils.getFileNamePrefix(originalCsvFile);
        File versionParquetFile = Utils.getParquetFile(prefix, version, "plain", true);
        File csvVersionedTestFile = Utils.getCsvTestFile(prefix, version, true);

        ConvertUtils.convertParquetToCSV(versionParquetFile, csvVersionedTestFile);

        Utils.verify(originalCsvFile, csvVersionedTestFile);
      }
    }
  }

  @Test
  public void testDumpMeta() throws Exception {
    // read all versions of parquet files and convert them into csv
    // diff the csvs with original csvs
    File[] originalCsvFiles = Utils.getAllOriginalCSVFiles();
    String[] compatibleVersions = Utils.getAllPreviousVersionDirs();
    String version = "1.9.0"; // compatibleVersions[0];
    String prefix = Utils.getFileNamePrefix(originalCsvFiles[0]);
    File versionOrcFile = Utils.getRefFile(prefix, version, "plain", true, ConvertToolBase.Format.PARQUET , null);

    File testMeta = Utils.getMetaTestFile(prefix, version, true, ConvertToolBase.Format.PARQUET);
    File refMeta = Utils.getMetaRefFile(prefix, version, null, ConvertToolBase.Format.PARQUET);

    StringRedirector redir = null;
    try {
      redir = new StringRedirector(StringRedirector.outputStream(testMeta));
      String[] args = { "meta", versionOrcFile.toString() };
      Driver.main(args);
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println(e);
      Assert.fail("exception" + e);
      throw e;
    } finally {
      redir.restore();
    }
    String[][] canonize = { { "^file:.*", "file:<afile>" }, { "PLAIN,BIT_PACKED", "BIT_PACKED,PLAIN" },
        { "RLE,PLAIN", "PLAIN,RLE" }, 
        { "RLE,BIT_PACKED", "BIT_PACKED,RLE" },
        { "PLAIN,BIT_PACKED", "BIT_PACKED,PLAIN" },
        { "RLE,PLAIN", "PLAIN,RLE" }, };
    Utils.verifyCanonize(refMeta, testMeta, canonize);
  }

  @Test
  public void testDumpSchema() throws Exception {
    // read all versions of parquet files and convert them into csv
    // diff the csvs with original csvs
    File[] originalCsvFiles = Utils.getAllOriginalCSVFiles();
    String version = "1.9.0"; // compatibleVersions[0];
    String prefix = Utils.getFileNamePrefix(originalCsvFiles[0]);
    File versionOrcFile = Utils.getRefFile(prefix, version, "plain", true, ConvertToolBase.Format.PARQUET, null);

    File testMeta = Utils.getExtTestFile(prefix, version, null, true, ConvertToolBase.Format.PARQUET, "schema");
    File refMeta = Utils.getExtRefFile(prefix, version, null, ConvertToolBase.Format.PARQUET, "schema");

    StringRedirector redir = null;
    try {
      redir = new StringRedirector(StringRedirector.outputStream(testMeta));
      String[] args = { "schema", versionOrcFile.toString() };
      Driver.main(args);
    } catch (Exception e) {
      throw e;
    } finally {
      redir.restore();
    }
    Utils.verify(refMeta, testMeta);
  }

  @Test
  public void testDumpSchemaDetailed() throws Exception {
    File[] originalCsvFiles = Utils.getAllOriginalCSVFiles();
    // String[] compatibleVersions = Utils.getAllPreviousVersionDirs(;
    String version = "1.9.0"; // compatibleVersions[0];
    String prefix = Utils.getFileNamePrefix(originalCsvFiles[0]);
    File versionOrcFile = Utils.getRefFile(prefix, version, "plain", true, ConvertToolBase.Format.PARQUET, null);

    File testMeta = Utils.getExtTestFile(prefix, version,null,  true, ConvertToolBase.Format.PARQUET, "schema.ext");
    File refMeta = Utils.getExtRefFile(prefix, version, null, ConvertToolBase.Format.PARQUET, "schema.ext");

    StringRedirector redir = null;
    try {
      redir = new StringRedirector(StringRedirector.outputStream(testMeta));
      String[] args = { "schema", versionOrcFile.toString(), "-x", "true" };
      Driver.main(args);
    } catch (Exception e) {
      throw e;
    } finally {
      redir.restore();
    }
    Utils.verify(refMeta, testMeta);
  }

}
