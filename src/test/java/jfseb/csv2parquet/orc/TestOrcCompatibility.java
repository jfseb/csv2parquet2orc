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

package jfseb.csv2parquet.orc;

import java.io.File;

import org.apache.parquet.Log;
import org.junit.Test;

import jfseb.csv2parquet.Driver;
import jfseb.csv2parquet.Utils;
import jfseb.csv2parquet.convert.ConvertToolBase;
import jfseb.csv2parquet.convert.ConvertToolBase.Format;
import jfseb.csv2parquet.parquet.StringRedirector;
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
public class TestOrcCompatibility {

  private static final Log LOG = Log.getLog(TestOrcCompatibility.class);

  @Test
  public void testReadWriteCompatibility() throws Exception {
    File[] csvFiles = Utils.getAllOriginalCSVFiles();
    for (File csvFile : csvFiles) {
      String filename = Utils.getFileNamePrefix(csvFile);

      // With no dictionary - default
      File orcTestFile = Utils.getOrcOutputFile(filename, "plain", true);
      String[] args = { "convert", "-S", "|", csvFile.toString(), "-o", orcTestFile.toString() };
      Driver.main(args);
      // ConvertUtils.convertCsvToParquet(csvFile, orcTestFile);
      File csvTestFile = Utils.getCsvTestFile(filename, "plain", true);

      // String[] compatibleVersions = Utils.getAllPreviousVersionDirs();
      String[] compatibleVersions = { "1.4.0" };
      for (String version : compatibleVersions) {
        LOG.info("Testing compatibility with " + version);
        String prefix = Utils.getFileNamePrefix(csvFile);
        File versionOrcFile = Utils.getRefOrcFile(prefix, version, "plain", true);
        Utils.verifyBinary(orcTestFile, versionOrcFile);
      }
      // convert back, not supported for orc
      /*
       * String[] args2 = { "convert", "-S","|", orcTestFile.toString(), "-o",
       * csvTestFile.toString() }; Driver.main(args2);
       * //ConvertUtils.convertParquetToCSV(orcTestFile, csvTestFile);
       * Utils.verify(csvFile, csvTestFile);
       */
    }

  }

  @Test
  public void testReadWriteTypesCompatibility() throws Exception {
    File[] csvFiles = Utils.getAllManyTypesCSVFiles();
    for (File csvFile : csvFiles) {
      String filename = Utils.getFileNamePrefix(csvFile);

      // With no dictionary - default
      File orcTestFile = Utils.getOrcOutputFile(filename, "plain", true);
      String[] args = { "convert", "-S", "|", csvFile.toString(), "-o", orcTestFile.toString() };
      Driver.main(args);
      // ConvertUtils.convertCsvToParquet(csvFile, orcTestFile);
      File csvTestFile = Utils.getCsvTestFile(filename, "plain", true);

      // String[] compatibleVersions = Utils.getAllPreviousVersionDirs();
      String[] compatibleVersions = { "1.4.0" };
      String prefix = Utils.getFileNamePrefix(csvFile);
      for (String version : compatibleVersions) {
        LOG.info("Testing compatibility with " + version);
        File versionOrcFile = Utils.getRefOrcFile(prefix, version, "plain", true);
        // file contains path Utils.verifyBinary(orcTestFile, versionOrcFile);
      }
      File testMeta = Utils.getMetaTestFile(prefix, compatibleVersions[0], true, ConvertToolBase.Format.ORC);
      File refMeta = Utils.getMetaRefFile(prefix, compatibleVersions[0], null, ConvertToolBase.Format.ORC);

      StringRedirector redir = null;
      try {
        redir = new StringRedirector(StringRedirector.outputStream(testMeta));
        String[] args2 = { "meta", orcTestFile.toString() };
        Driver.main(args2);
      } catch (Exception e) {
        Assert.fail("exception" + e);
        throw e;
      } finally {
        redir.restore();
      }
      Utils.verify(refMeta, testMeta);
    }
    // convert back, not supported for orc
    /*
     * String[] args2 = { "convert", "-S","|", orcTestFile.toString(), "-o",
     * csvTestFile.toString() }; Driver.main(args2);
     * //ConvertUtils.convertParquetToCSV(orcTestFile, csvTestFile);
     * Utils.verify(csvFile, csvTestFile);
     */
  }

  /*
   * // With dictionary encoding orcTestFile =
   * Utils.getParquetOutputFile(filename, "dict", true); String[] args2 = {
   * "--define", "orc.compress=SNAPPY", "convert", csvFile.toString(), "-o",
   * orcTestFile.toString() }; Driver.main(args2);
   * 
   * ConvertUtils.convertCsvToParquet(csvFile, orcTestFile, true); csvTestFile =
   * Utils.getCsvTestFile(filename, "dict", true);
   * ConvertUtils.convertParquetToCSV(orcTestFile, csvTestFile);
   * 
   * Utils.verify(csvFile, csvTestFile); } }
   * 
   * public class OutputRedirector { / * args[0] - class to launch,
   * args[1]/args[2] file to direct System.out/System.err to * / public static
   * void main(String[] args) throws Exception { // error checking omitted for
   * brevity System.setOut(outputFile(args(1)); System.setErr(outputFile(args(2));
   * Class app = Class.forName(args[0]); Method main =
   * app.getDeclaredMethod("main", new Class[] { (new String[1]).getClass()});
   * String[] appArgs = new String[args.length-3]; System.arraycopy(args, 3,
   * appArgs, 0, appArgs.length); main.invoke(null, appArgs); } protected
   * PrintStream outputFile(String name) { return new PrintStream(new
   * BufferedOutputStream(new FileOutputStream(name))); } }
   * 
   * 
   */

  @Test
  public void testOrcDumpMeta() throws Exception {
    // read all versions of parquet files and convert them into csv
    // diff the csvs with original csvs
    File[] originalCsvFiles = Utils.getAllOriginalCSVFiles();
    String[] compatibleVersions = Utils.getAllPreviousVersionDirs();
    String version = "1.4.0"; // compatibleVersions[0];
    String prefix = Utils.getFileNamePrefix(originalCsvFiles[0]);
    File versionOrcFile = Utils.getRefOrcFile(prefix, version, "plain", true);

    File testMeta = Utils.getMetaTestFile(prefix, version, true, ConvertToolBase.Format.ORC);
    File refMeta = Utils.getMetaRefFile(prefix, version, null, ConvertToolBase.Format.ORC);

    StringRedirector redir = null;
    try {
      redir = new StringRedirector(StringRedirector.outputStream(testMeta));
      String[] args = { "meta", versionOrcFile.toString() };
      Driver.main(args);
    } catch (Exception e) {
      Assert.fail("exception" + e);
      throw e;
    } finally {
      redir.restore();
    }
    Utils.verify(refMeta, testMeta);
  }

  @Test
  public void testDumpSchemaDetailed() throws Exception {
    File[] originalCsvFiles = Utils.getAllOriginalCSVFiles();
    String[] compatibleVersions = Utils.getAllPreviousVersionDirs(Format.ORC);
    String version = compatibleVersions[0];
    String prefix = Utils.getFileNamePrefix(originalCsvFiles[0]);
    File versionOrcFile = Utils.getRefFile(prefix, version, "plain", true, ConvertToolBase.Format.ORC, null);

    File testMeta = Utils.getExtTestFile(prefix, version, null, true, ConvertToolBase.Format.ORC, "schema.ext");
    File refMeta = Utils.getExtRefFile(prefix, version, null, ConvertToolBase.Format.ORC, "schema.ext");
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

  @Test
  public void testOrcBackwardsCompatibility() throws Exception {
    // read all versions of parquet files and convert them into csv
    // diff the csvs with original csvs
    File[] originalCsvFiles = Utils.getAllOriginalCSVFiles();
    String[] compatibleVersions = Utils.getAllPreviousVersionDirs();
    // String[] compatibleVersions = {"1.4.0"};
    for (String version : compatibleVersions) {
      LOG.info("Testing compatibility with " + version);
      for (File originalCsvFile : originalCsvFiles) {

        String prefix = Utils.getFileNamePrefix(originalCsvFile);
        File versionOrcFile = Utils.getRefOrcFile(prefix, version, "plain", true);
        File csvVersionedTestFile = Utils.getCsvTestFile(prefix, version, true);
        String[] args = { "convert", "-S", "|", versionOrcFile.toString(), "-o", csvVersionedTestFile.toString() };
        Driver.main(args);
        // ConvertUtils.convertOrcToCSV(versionOrcFile, csvVersionedTestFile);

        Utils.verify(originalCsvFile, csvVersionedTestFile);
      }
    }
  }
}
