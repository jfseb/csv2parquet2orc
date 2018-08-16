# csv2parquet   [![Build Status](https://travis-ci.org/jfseb/csv2parquet2orc.svg?branch=master)](https://travis-ci.org/jfseb/csv2parquet2orc)

CSV 2 Parquet and CSV 2 ORC converter 
(blend of individual tools with aligned interface) 

#build 

mvn clean compile assembly:single

mvn test

# 

schema <orc|parquet-file>   [-x|--extended]

# execute: 


## csv to parquet

java -jar csv2parquet2orc-0.0.4-*   convert  -D parquet.compression=GZIP   input.csv  -s input.csv.schema -o out.parquet 

options: 
 *  parquet.dictionary  true|false
 * -D parquet.DEFAULT_BLOCK_SIZE <int> 
 * -D parquet.DEFAULT_PAGE_SIZE <int>

 * -S '|' csv column separator
 
 * -D csvformat=binary   reads columns matching the pattern /0x([A-Fa-f0-9][A-Fa-f0-9])+x0/,
   e.g.  |0xFFEFx0|0xffefx0|0x41x0|
   for the latter format, all columns starting with 0x and ending with x0  (e.g. 0xFFEFx0 
   and containing an even number of contiguous hexadecimal characters  will be interpreted as 
   binary representation.
   The data will be interpreted as Big-Endian representation of the data  
    (7FFF) is the value 32768 = 0x7FFF etc. 
   Where needed, it will be left-padded with 00 or truncated to the target width. 
   Subsequently is is interpreted as the binary representation of the data

## csv to orc 

java -jar csv2parquet2orc-0.0.2-*   convert  -D orc.compression=ZIP   input.csv  -s input.csv.schema -o out.orc 


## other commands

### --help output help

### meta output file metadata

java -jar csv2parquet2orc-0.0.2-*   meta  abc.parquet

java -jar csv2parquet2orc-0.0.2-*   meta  abc.orc


# notes

The project is built on parquet 1.9.0 
and orc 1.4 
using sources from [https://github.com/Parquet/parquet-compatibility]
and                [https://github.com/apache/orc/tree/master/tools]


#

# License

Apache License 2.0


# Windows: 

to operate the functionality on windows, one may have to set 
environment variable HADOOP_HOME and install at least  
  winutils.exe in HADOOP_HOME  there. 
  
  example:

 - HADOOP_HOME=   c:\progs\hadoop
 - PATH= %PATH%;HADOOP_HOME\bin 

(A full hadoop installation is not required)




