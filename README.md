# csv2parquet   [![Build Status](https://travis-ci.org/jfseb/csv2parquet2orc.svg?branch=master)](https://travis-ci.org/jfseb/csv2parquet2orc)

CSV 2 Parquet and CSV 2 ORC converter 
(blend of individual tools with aligned interface) 

- csv to parquet conversion
- csv to orc conversion
 
- listing of meta information for orc/parquet (schema, statistics, encoding choices)
- control some serialization formats (e.g. Strip size/BLock length, dictionary enable/disable) 

- special features for generating test data
 * allows binary notation of input in CSV 
   to force specific values into the parquet/orc file for test purposes
   (e.g. various float/double NAN, "out of range" int96 julian dates, other dates/timestamp, broken or 
    wrong encoded unicode charaters etc.)
 * allows to write int96 "Impala" timestamps



<!-- toc -->
- [build](#build)
- [run](#run)
  
<!-- tocstop -->


# build 

```
% mvn clean compile assembly:single
% mvn test
```

the mvn build builds a single jar with all dependencies packaged 
as  `target/csv2parquet2orc-0.0.X-SNAPSHOT-jar-with-dependencies.jar`
 

Note: the project can be built on a jdk8 and jdk7 environment, see here
<https://travis-ci.org/jfseb/csv2parquet2orc>

Note: one requires a jdk with tools.jar present, e.g. oracle 8 jdk, sapjvm sdk
 and may have to set JAVA_HOME to point at the JDK or install a default, not openjdk jdk
 see <https://stackoverflow.com/questions/5730815/unable-to-locate-tools-jar>
 
tools.jar was removed with oracle 9. Feel free to enable an JDK9 specific build variant 
and provide a pull request. Thanks. 
 


# run 

## display schema of file

```
% java -jar csv2parquet2orc-0.0.4-*.jar  schema afile.orc      [-x|--extended]
% java -jar csv2parquet2orc-0.0.4-*.jar  schema afile.parquet  [-x|--extended]
``` 

## display meta information of a file

```
% java -jar csv2parquet2orc-0.0.4-*.jar meta abc.orc
% java -jar csv2parquet2orc-0.0.4-*.jar meta abc.parquet
``` 


## csv to parquet

```
% java -jar csv2parquet2orc-0.0.4-*   convert  -D parquet.compression=GZIP  input.csv  -s input.csv.schema -o out.parquet -S '|'
```


options: 
 *  parquet.dictionary  true|false
 * -D parquet.DEFAULT_BLOCK_SIZE <int> 
 * -D parquet.DEFAULT_PAGE_SIZE <int>

 * -S '|' csv column separator, default ','
 * -H 1   skip 1 line in csv (e.g. header line)
 
 
## csv to orc 

```
java -jar csv2parquet2orc-0.0.2-*   convert  -D orc.compression=ZIP   input.csv  -s input.csv.schema -o out.orc 
```

## some csv options

the following is a subset of options

| option | option example | parquet | orc | Comment
| --- | --- | --- | --- |
| explicit schema file spec | -s abc.schema.orc | yes | yes |
| Skip header lines | -H 1 | yes | yes |
| Separator | -S '\|' | yes | yes | 
| Binary 0x12EFx0 (1) |  -Dcsvformat=binary | yes | yes(2) |


 * (1) -D csvformat=binary shall be entered before the command  |
 * (2) Timestamp and decimal writing for orc via semi-typed classes of orc reader which may limit 
       full byte range


## csv binary notation

 when setting -D csvformat=binary  
  csv columns matching the pattern /0x(\[A-Fa-f0-9\]\[A-Fa-f0-9\])+x0/,
   e.g.  |0xFFFFFFFFx0|0xffefx0|0x41x0| 
   are interpreted as binary data. 
   
   The converter will take the binary data 
   ( left-padding it with 0x00 
   or truncating it from the left where required ) 
   as big-endian data and move it into the respective column, 
  
   so 0x41x0  will yield 'A' on a varchar column, 
   65 on an integer/long/ column etc. 
   
   
   for the latter format, all columns starting with 0x and ending with x0  (e.g. 0xFFEFx0 
   and containing an even number of contiguous hexadecimal characters  will be interpreted as 
   binary representation.
   The data will be interpreted as Big-Endian representation of the data  
    (7FFF) is the value 32768 = 0x7FFF etc. 
   Where needed, it will be left-padded with 00 or truncated to the target width. 
   Subsequently is is interpreted as the binary representation of the data


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


## Int96 timestamp aka impala timestamps

The converter used the messed up "Hive" julian calendar conversion calculation 

so for this reader  
```
Hive converts "1970-01-01 00:00:00.0" to Julian timestamp:
(julianDay=2440588, timeOfDayNanos=0)
```
see
<https://issues.apache.org/jira/browse/HIVE-6394?focusedCommentId=14711046&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-14711046>


*Beware:*
  in contrast to other parquet data the int96 impala timestamp is stored in little-endian order: 
  ```
    0x<nanos in little endian (int64_t, 8 bytes)><juliandays in little endian (int32_t)>0x
  ```
   Example:
   * 16 nanos after 1970-01-01 00:00:00.0000000 :
   * juliandays: 2440588 = 0x253D8C ; timeOfDayNanos: 16 = 0x10;
   are represented by bytes:	
   
  ``` 
  |0x01000000000000008C3D2500x0|
  <nanos LE;8 bytes  >  <days LE 4b>
  |0x 0100 0000 0000 0000  8C 3D 25 00   x0|
  ```

  (Normal integers/date/time/character strings etc. are to be written in big-endian order, so the number 
     2440588 would be  `0x00253D8Cx0` , "ABC" is `0x414243x0` etc..)
   
   
 # example schemas 
 
 
 
 ## parquet schema 
 ```
 message m {
  OPTIONAL int32 d32 (DATE);
  OPTIONAL binary c3 (UTF8);
  OPTIONAL int64  d9_5 (DECIMAL(9,5);  
  OPTIONAL binary d3_8 (DECIMAL(38,5);  
  OPTIONAL int32 ui16 (UINT_32);
  OPTIONAL int32 i32;
  OPTIONAL int32 t_millis (TIME_MILLIS) ;
  OPTIONAL int64 t_micros (TIME_MICROS);
  OPTIONAL int64 ts_millis (TIMESTAMP_MILLIS) ;
  OPTIONAL int64 ts_micros (TIMESTAMP_MICROS);
  OPTIONAL int96 ts_i96  (TIMESTAMP);
  OPTIONAL double dbl;
  OPTIONAL float  flt;  
  OPTIONAL int64 plain;
  OPTIONAL binary c4 (UTF8);
 }
 ```

## orc schema

struct<cust_key:int,name:string,nation_keys:smallint,acctbal:double,adate:date,adec:decimal(9,5)>

run meta <filename> on 
https://github.com/apache/orc/tree/master/examples 
