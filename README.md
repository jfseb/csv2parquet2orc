# csv2parquet   [![Build Status](https://travis-ci.org/jfseb/csv2parquet2orc.svg?branch=master)](https://travis-ci.org/jfseb/csv2parquet2orc)

CSV 2 Parquet and CSV 2 ORC converter 
(blend of individual tools with aligned interface) 

#build 

mvn clean compile assembly:single

mvn test

# 

schema <orc|parquet-file>   [-x|--extended]

# execute: 


##csv to parquet

java -jar csv2parquet2orc-0.0.2-*   convert  -D parquet.compression=GZIP   input.csv  -s input.csv.schema -o out.parquet 

options: 
   parquet.dictionary  true|false
-D parquet.DEFAULT_BLOCK_SIZE <int> 
-D parquet.DEFAULT_PAGE_SIZE <int>

-D csvformat=binary   reads columns as e.g.  |FFEF|FFEF 

##cv to orc 

java -jar csv2parquet2orc-0.0.2-*   convert  -D orc.compression=ZIP   input.csv  -s input.csv.schema -o out.orc 




The project is built on parquet 1.9.0 
and orc 1.4 
using sources from [https://github.com/Parquet/parquet-compatibility]
and                [https://github.com/apache/orc/tree/master/tools]


#License

Apache License 2.0


# Windows: 

to operate the functionality on windows, one may have to set 
environment varialbe HADOOP_HOME and install at least  
  winutils.exe in HADOOP_HOME  there. 
  
  example:

 - HADOOP_HOME=   c:\progs\hadoop
 - PATH= %PATH%;HADOOP_HOME\bin 

(A full hadoop installation is not required)




