package jfseb.csv2parquet;

import org.apache.orc.tools.Driver;

public class PackageVisAdapter {

  public static void ScanData_main(String[] args) throws Exception {
    Driver.main(args);
  }

  public static void PrintData_main(String[] args) throws Exception {
    Driver.main(args);
  }

  public static void FileDump_main(String[] args) throws Exception {
    Driver.main(args);
    //FileDump.main(conf, options.commandArgs);
  }
  
}
