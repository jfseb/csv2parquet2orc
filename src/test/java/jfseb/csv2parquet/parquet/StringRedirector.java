package jfseb.csv2parquet.parquet;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

import org.apache.parquet.tools.Main;

public class StringRedirector {

  
  public static PrintStream outputStream(File name) throws FileNotFoundException {
    return new PrintStream(new BufferedOutputStream(new FileOutputStream(name)));
  }
  
  final PrintStream oldStdOut;
  final PrintStream oldStdErr;
  final PrintStream newOut;
  final PrintStream oldMainOut;
  
  public StringRedirector(PrintStream anewOut) {
    this.oldMainOut = Main.out;
    this.oldStdOut = System.out;
    this.oldStdErr = System.err;
    this.newOut = anewOut;
    Main.out = anewOut;
    System.setOut(newOut);
   // System.setErr(newOut);
  }

  public void restore() {
    Main.out = this.oldMainOut;
    System.setOut(oldStdOut);
   // System.setErr(oldStdErr);
    newOut.close();
  }
}