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
package jfseb.csv2parquet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.file.tfile.Utils.Version;
import org.apache.parquet.Log;

import jfseb.csv2parquet.convert.ConvertToolBase;
import jfseb.csv2parquet.convert.ConvertToolBase.Format;

public class Utils {

  private static final Log LOG = Log.getLog(Utils.class);

  public static void closeQuietly(Closeable res) {
    try {
      if (res != null) {
        res.close();
      }
    } catch (IOException ioe) {
      LOG.warn("Exception closing reader " + res + ": " + ioe.getMessage());
    }
  }

  public static void writePerfResult(String module, long millis) throws IOException {
    PrintWriter writer = null;
    try {
      File outputFile = new File("target/test/perftime." + module + ".txt");
      outputFile.delete();
      writer = new PrintWriter(outputFile);
      writer.write(String.valueOf(millis));
    } finally {
      closeQuietly(writer);
    }
  }

  public static long readPerfResult(String version, String module) throws IOException {
    BufferedReader reader = null;
    try {
      File inFile = new File("../" + version + "/target/test/perftime." + module + ".txt");
      reader = new BufferedReader(new FileReader(inFile));
      return Long.parseLong(reader.readLine());
    } finally {
      closeQuietly(reader);
    }
  }

  public static File createTestFile(long largerThanMB) throws IOException {
    File outputFile = new File("target/test/csv/perftest.csv");
    if (outputFile.exists()) {
      return outputFile;
    }
    File toCopy = new File("./parquet-testdata/tpch/customer.csv");
    FileUtils.copyFile(new File("./parquet-testdata/tpch/customer.parquet.schema"),
        new File("target/test/csv/perftest.parquet.schema"));

    OutputStream output = null;
    InputStream input = null;

    try {
      output = new BufferedOutputStream(new FileOutputStream(outputFile, true));
      input = new BufferedInputStream(new FileInputStream(toCopy));
      input.mark(Integer.MAX_VALUE);
      while (outputFile.length() <= largerThanMB * 1024 * 1024) {
        // appendFile(output, toCopy);
        IOUtils.copy(input, output);
        input.reset();
      }
    } finally {
      closeQuietly(input);
      closeQuietly(output);
    }

    return outputFile;
  }

  public static File[] getAllOriginalCSVFiles() {
    File baseDir = new File("./parquet-testdata/tpch");
    final File[] csvFiles = baseDir.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.endsWith(".csv");
      }
    });
    return csvFiles;
  }


  public static File[] getAllBinaryCSVFiles() {
    File baseDir = new File("./parquet-testdata/csvbinary");
    final File[] csvFiles = baseDir.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.endsWith(".csv");
      }
    });
    return csvFiles;
  }

  
  public static File[] getAllManyTypesCSVFiles() {
    File baseDir = new File("./parquet-testdata/manytypes");
    final File[] csvFiles = baseDir.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.endsWith(".csv");
      }
    });
    return csvFiles;
  }

  public static String[] getAllPreviousVersionDirs() throws IOException {
    File baseDir = new File("..");
    final String currentVersion = getCurrentVersion();
    final String[] versions = baseDir.list(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.startsWith("parquet-compat-")
            && new Version(name.replace("parquet-compat-", "")).compareTo(new Version(currentVersion)) < 0;
      }
    });
    return versions;
  }

  static class Version implements Comparable<Version> {
    int major;
    int minor;
    int minorminor;
    String tag;

    Version(String versionStr) {
      String[] versions = versionStr.split("\\.");
      int size = versions.length;
      if (size > 0) {
        this.major = Integer.parseInt(versions[0]);
      }
      if (size > 1) {
        this.minor = Integer.parseInt(versions[1]);
      }
      if (size > 2) {
        if (versions[2].contains("-")) {
          String[] minorMin = versions[2].split("-");
          this.minorminor = Integer.parseInt(minorMin[0]);
          this.tag = minorMin[1];
        } else {
          this.minorminor = Integer.parseInt(versions[2]);
        }
      }
      if (size == 4) {
        this.tag = versions[3];
      }
      if (size > 4) {
        throw new RuntimeException("Illegal version number " + versionStr);
      }
    }

    public int compareMajorMinor(Version o) {
      return ComparisonChain.start().compare(major, o.major).compare(minor, o.minor).result();
    }

    @Override
    public int compareTo(Version o) {
      return (int) ComparisonChain.start().compare(major, o.major).compare(minor, o.minor)
          .compare(minorminor, o.minorminor).compare(tag, o.tag).result();
    }

    // Very basic implementation of comparisonchain
    private static class ComparisonChain {
      int result = 0;

      private ComparisonChain(int result) {
        this.result = result;
      }

      static ComparisonChain start() {
        return new ComparisonChain(0);
      }

      ComparisonChain compare(String a, String b) {
        if (result != 0) {
          return this;
        }
        if (b == null) {
          if (a != null)
            result = 1;
          else
            result = 0;
        } else if (a == null) {
          result = 1;
        } else if (result == 0) {
          result = a.compareTo(b);
        }
        return this;
      }

      ComparisonChain compare(int a, int b) {
        if (result == 0) {
          result = Integer.compare(a, b);
        }
        return this;
      }

      int result() {
        return result;
      }
    }
  }

  public static File getParquetOutputFile(String name, String module, boolean deleteIfExists) {
    File outputFile = new File("target/parquet/", getParquetFileName(name, module));
    outputFile.getParentFile().mkdirs();
    if (deleteIfExists) {
      outputFile.delete();
    }
    return outputFile;
  }

  public static File getOrcOutputFile(String name, String module, boolean deleteIfExists) {
    File outputFile = new File("target/orc/", getFormatFileName(name, module, Format.ORC));
    outputFile.getParentFile().mkdirs();
    if (deleteIfExists) {
      outputFile.delete();
    }
    return outputFile;
  }

  private static String getParquetFileName(String name, String module) {
    return getFormatFileName(name, module, Format.PARQUET);
  }

  private static String getOrcFileName(String name, String module) {
    return getFormatFileName(name, module, Format.ORC);
  }

  private static String getFormatFileName(String name, String module, Format format) {
    return name + (module != null ? "." + module : "") + ConvertToolBase.toExtension(format);
  }

  public static File getParquetFile(String name, String version, String module, boolean failIfNotExist)
      throws IOException {
    return getRefFile(name, version, module, failIfNotExist, Format.PARQUET, null);
    /*
     * File parquetFile = new File("../"+version+"/target/parquet/",
     * getParquetFileName(name, module)); parquetFile.getParentFile().mkdirs();
     * if(!parquetFile.exists()) { String msg = "File " +
     * parquetFile.getAbsolutePath() + " does not exist"; if(failIfNotExist) { throw
     * new IOException(msg); } LOG.warn(msg); } return parquetFile;
     */
  }

  public static File getRefOrcFile(String name, String version, String module, boolean failIfNotExist)
      throws IOException {
    return getRefFile(name, version, module, failIfNotExist, Format.ORC, null);
  }

  public static File getRefFile(String name, String version, String module, boolean failIfNotExist, Format format,
      String ext) throws IOException {

    File parquetFile = new File(
        ConvertToolBase.getDefaultFormatName(format) + "-testdata/" + ((version != null) ? version + "/" : ""),
        getFormatFileName(name, module, format) + ((ext != null) ? "." + ext : ""));
    parquetFile.getParentFile().mkdirs();
    File osVariantFile = makeOSVariantName(parquetFile);
    if (osVariantFile.exists()) {
      return osVariantFile;
    }
    if (!parquetFile.exists()) {
      String msg = "File " + parquetFile.getAbsolutePath() + " does not exist";
      if (failIfNotExist) {
        throw new IOException(msg);
      }
      LOG.warn(msg);
    }
    return parquetFile;
  }

  private static File makeOSVariantName(File parquetFile) {
    List<String> ext = new ArrayList<String>(Arrays.asList(parquetFile.getName().split("\\.")));
    String extOS = getOSVariantExtension();
    ext.add(Math.max(0, ext.size() - 1), extOS);
    String[] arr = {};
    return new File(parquetFile.getParentFile().toString() + "/" + String.join(".", ext));
  }

  public static File getExtRefFile(String name, String version, String module, Format format, String ext)
      throws IOException {
    return getRefFile(name, version, module, true, format, ext);
    /*
     * 
     * File outputFile = new File(ConvertToolBase.getDefaultFormatName(format) +
     * "-testdata" + (module != null ? "/" + module : "") + "/" + name + (module !=
     * null ? "." + module : "") + "." +
     * ConvertToolBase.getDefaultFormatName(format) + "." + ext); return outputFile;
     */
  }

  private static String getCurrentVersion() throws IOException {
    return "1.2.0"; // new File(".").getCanonicalFile().getName().replace("parquet-compat-", "");
  }

  public static String[] getImpalaDirectories() throws IOException {
    File baseDir = new File("parquet-testdata/impala");
    final String currentVersion = getCurrentVersion();
    final String[] impalaVersions = baseDir.list(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        if (name.startsWith(".")) {
          return false;
        }
        if (name.contains("-")) {
          name = name.split("-")[0];
        }
        return new Version(name).compareMajorMinor(new Version(currentVersion)) == 0;
      }
    });
    return impalaVersions;
  }

  public static File getParquetImpalaFile(String name, String impalaVersion) throws IOException {
    String fileName = name + ".impala.parquet";
    File parquetFile = new File("parquet-testdata/impala/" + impalaVersion, fileName);
    if (!parquetFile.exists()) {
      throw new IOException("File " + fileName + " does not exist");
    }
    return parquetFile;
  }

  public static String getFileNamePrefix(File file) {
    return file.getName().substring(0, file.getName().indexOf("."));
  }

  public static File getCsvTestFile(String name, String module, boolean deleteIfExists) {
    File outputFile = new File("target/test/csv/", name + (module != null ? "." + module : "") + ".csv");
    outputFile.getParentFile().mkdirs();
    if (deleteIfExists) {
      outputFile.delete();
    }
    return outputFile;
  }

  public static File getExtTestFile(String name, String version, String module, boolean deleteIfExists, Format format,
      String ext) {
    File outputFile = new File("target/test/" + ((version != null) ? version + "/" : ""),
        name + (module != null ? "." + module : "") + "." + ConvertToolBase.getDefaultFormatName(format) + "." + ext);
    outputFile.getParentFile().mkdirs();
    if (deleteIfExists) {
      outputFile.delete();
    }
    return outputFile;
  }

  public static File getMetaTestFile(String name, String version, boolean deleteIfExists, Format format) {
    return getExtTestFile(name, version, null, deleteIfExists, format, "meta");
  }

  private static String getOSVariantExtension() {
    boolean isWin = System.getProperty("os.name").startsWith("Windows");
    if (isWin) {
      return "win";
    }
    return "unix";
  }

  public static File getMetaRefFile(String name, String version, String module, Format format) throws IOException {
    return getExtRefFile(name, version, module, format, "meta");
  }

  public static File getSchemaRefFile(String name, String version, String module, Format format) throws IOException {
    return getExtRefFile(name, version, module, format, "schema");
  }

  public static File getParquetTestFile(String name, String module, boolean deleteIfExists) {
    File outputFile = new File("target/test/parquet/", name + (module != null ? "." + module : "") + ".csv");
    outputFile.getParentFile().mkdirs();
    if (deleteIfExists) {
      outputFile.delete();
    }
    return outputFile;
  }

  public static void verify(File expectedCsvFile, File outputCsvFile) throws IOException {
    BufferedReader expected = null;
    BufferedReader out = null;
    try {
      expected = new BufferedReader(new FileReader(expectedCsvFile));
      out = new BufferedReader(new FileReader(outputCsvFile));
      String lineIn = null;
      String lineOut = null;
      int lineNumber = 0;
      try {
        while ((lineIn = expected.readLine()) != null && (lineOut = out.readLine()) != null) {
          ++lineNumber;
          lineOut = lineOut.substring(lineOut.indexOf("\t") + 1);
          assertEquals("line " + lineNumber, lineIn, lineOut);
        }
      } catch (org.junit.ComparisonFailure cf) {
        throw new AssertionError("difference in files \n" + expectedCsvFile.toString() + "\n" + outputCsvFile.toString() + "\n" + cf.toString(), cf);
      }
      assertNull("line " + lineNumber, lineIn);
      assertNull("line " + lineNumber, out.readLine());
    } finally {
      Utils.closeQuietly(expected);
      Utils.closeQuietly(out);
    }
  }

  public static void verifyCanonize(File expectedCsvFile, File outputCsvFile, String[][] canonic) throws IOException {
    BufferedReader expected = null;
    BufferedReader out = null;
    String head = " file exp:" + expectedCsvFile.getCanonicalPath() + "\nfile out:: " + outputCsvFile.getCanonicalPath() ;
    try {
      expected = new BufferedReader(new FileReader(expectedCsvFile));
      out = new BufferedReader(new FileReader(outputCsvFile));
      String lineIn;
      String lineOut = null;
      int lineNumber = 0;
      while ((lineIn = expected.readLine()) != null && (lineOut = out.readLine()) != null) {
        ++lineNumber;
        lineOut = lineOut.substring(lineOut.indexOf("\t") + 1);
        String lineIn2 = canonize(canonic, lineIn);
        String lineOut2 = canonize(canonic, lineOut);
        assertEquals(head + "\nline " + lineNumber, lineIn2, lineOut2);
      }
      assertNull("line " + lineNumber, lineIn);
      assertNull("line " + lineNumber, out.readLine());
    } finally {
      Utils.closeQuietly(expected);
      Utils.closeQuietly(out);
    }
  }

  private static String canonize(String[][] canonic, String lineOut) {
    String lnNext = lineOut;
    for (int i = 0; i < canonic.length; ++i) {
      String[] can = canonic[i];
      lnNext = lnNext.replaceAll(can[0], can[1]);
    }
    return lnNext;
  }

  static byte[] readBinaryFile(String fileName) throws IOException {
    Path path = Paths.get(fileName);
    return Files.readAllBytes(path);
  }

  public static void verifyBinary(File file1, File file2) throws IOException {

    byte[] data1 = readBinaryFile(file1.toString());
    byte[] data2 = readBinaryFile(file2.toString());
    try {
      assertEquals(data1.length, data2.length);
    } catch (AssertionError as) {
      throw new AssertionError("files not equal \n" + file1.toString() + "\n" + file2.toString(), as);
    }
    // assertEquals(data1,data2);
  }

  public static void verify(File expectedCsvFile, File outputCsvFile, boolean orderMatters) throws IOException {
    if (!orderMatters) {
      // sort the files before diff'ing them
      expectedCsvFile = sortFile(expectedCsvFile);
      outputCsvFile = sortFile(outputCsvFile);
    }
    verify(expectedCsvFile, outputCsvFile);
  }

  private static File sortFile(File inFile) throws IOException {
    File sortedFile = new File(inFile.getAbsolutePath().concat(".sorted"));
    BufferedReader reader = new BufferedReader(new FileReader(inFile));
    PrintWriter out = new PrintWriter(new FileWriter(sortedFile));

    try {
      String inputLine;
      List<String> lineList = new ArrayList<String>();
      while ((inputLine = reader.readLine()) != null) {
        lineList.add(inputLine);
      }
      Collections.sort(lineList);

      for (String outputLine : lineList) {
        out.println(outputLine);
      }
      out.flush();
    } finally {
      closeQuietly(reader);
      closeQuietly(out);
    }
    return sortedFile;
  }

  public static String[] getAllPreviousVersionDirs(Format orc) {
    if (Format.ORC == orc) {
      String[] res = { "1.4.0" };
      return res;
    }
    return null;
  }
}
