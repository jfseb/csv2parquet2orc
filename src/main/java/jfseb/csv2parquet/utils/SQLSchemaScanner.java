package jfseb.csv2parquet.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;


/**
 * Scans simle SQL schemas
 * @author
 *
 */
public class SQLSchemaScanner {

 
  static public class SQLType {
    java.sql.Types sqltype;
    int length; 
    int precision;
    public PrimitiveType primitiveType;
    public String name;
  }

  public static SQLType extractSQLType(String line) {
    Pattern ptnHasNull = Pattern.compile("\\s+NULL", Pattern.CASE_INSENSITIVE);
    boolean hasNull = ptnHasNull.matcher(line).find();
    Repetition repetition = hasNull ? Repetition.OPTIONAL : Repetition.REQUIRED; 
    Pattern reCV = Pattern.compile("([A-Za-z0-9_]+)\\s+((CHARACTER\\s+VARYING)|VARCHAR|NVARCHAR)\\((\\d+)\\)",Pattern.CASE_INSENSITIVE);
    Matcher m = reCV.matcher(line);
    if(m.find()) {
      SQLType tt = new SQLType();
      tt.name = m.group(1);
      tt.length = Integer.parseInt(m.group(4));
      tt.primitiveType = new PrimitiveType(repetition,  PrimitiveTypeName.BINARY, tt.length, tt.name);
      return tt; 
    } 
    reCV = Pattern.compile("([A-Za-z0-9_]+)\\s+((TIMESTAMP))",Pattern.CASE_INSENSITIVE);
    m = reCV.matcher(line);
    if(m.find()) {
      SQLType tt = new SQLType();
      tt.name = m.group(1);
      tt.length = 0;
      tt.primitiveType = new PrimitiveType(repetition,  PrimitiveTypeName.INT96,tt.name,OriginalType.TIMESTAMP_MICROS); 
      return tt; 
    } 
    reCV = Pattern.compile("([A-Za-z0-9_]+)\\s+((TIME))",Pattern.CASE_INSENSITIVE);
    m = reCV.matcher(line);
    if(m.find()) {
      SQLType tt = new SQLType();
      tt.name = m.group(1);
      tt.length = 0;
      tt.primitiveType = new PrimitiveType(repetition,  PrimitiveTypeName.INT64,tt.name,OriginalType.TIME_MILLIS); 
      return tt; 
    }
    reCV = Pattern.compile("([A-Za-z0-9_]+)\\s+((DATE))",Pattern.CASE_INSENSITIVE);
    m = reCV.matcher(line);
    if(m.find()) {
      SQLType tt = new SQLType();
      tt.name = m.group(1);
      tt.length = 0;
      tt.primitiveType = new PrimitiveType(repetition,  PrimitiveTypeName.INT64,tt.name,OriginalType.DATE); 
      return tt; 
    } 
    reCV = Pattern.compile("([A-Za-z0-9_]+)\\s+((TINYINT))",Pattern.CASE_INSENSITIVE);
    m = reCV.matcher(line);
    if(m.find()) {
      SQLType tt = new SQLType();
      tt.name = m.group(1);
      tt.length = 0;
      tt.primitiveType = new PrimitiveType(repetition,  PrimitiveTypeName.INT32,tt.name,OriginalType.INT_16); 
      return tt; 
    } 
    reCV = Pattern.compile("([A-Za-z0-9_]+)\\s+((SHORTINT))",Pattern.CASE_INSENSITIVE);
    m = reCV.matcher(line);
    if(m.find()) {
      SQLType tt = new SQLType();
      tt.name = m.group(1);
      tt.length = 0;
      tt.primitiveType = new PrimitiveType(repetition,  PrimitiveTypeName.INT32,tt.name,OriginalType.INT_32); 
      return tt; 
    } 
    reCV = Pattern.compile("([A-Za-z0-9_]+)\\s+((INT))",Pattern.CASE_INSENSITIVE);
    m = reCV.matcher(line);
    if(m.find()) {
      SQLType tt = new SQLType();
      tt.name = m.group(1);
      tt.length = 0;
      tt.primitiveType = new PrimitiveType(repetition,  PrimitiveTypeName.INT64,tt.name,OriginalType.INT_64); 
      return tt; 
    } 
    reCV = Pattern.compile("([A-Za-z0-9_]+)\\s+((DOUBLE))",Pattern.CASE_INSENSITIVE);
    m = reCV.matcher(line);
    if(m.find()) {
      SQLType tt = new SQLType();
      tt.name = m.group(1);
      tt.length = 0;
      tt.primitiveType = new PrimitiveType(repetition,  PrimitiveTypeName.DOUBLE,tt.name); 
      return tt; 
    }  
    reCV = Pattern.compile("([A-Za-z0-9_]+)\\s+((FLOAT))",Pattern.CASE_INSENSITIVE);
    m = reCV.matcher(line);
    if(m.find()) {
      SQLType tt = new SQLType();
      tt.name = m.group(1);
      tt.length = 0;
      tt.primitiveType = new PrimitiveType(repetition,  PrimitiveTypeName.FLOAT,tt.name); 
      return tt; 
    }  
    reCV = Pattern.compile("([A-Za-z0-9_]+)\\s+((BOOL))",Pattern.CASE_INSENSITIVE);
    m = reCV.matcher(line);
    if(m.find()) {
      SQLType tt = new SQLType();
      tt.name = m.group(1);
      tt.length = 0;
      tt.primitiveType = new PrimitiveType(repetition,  PrimitiveTypeName.BOOLEAN,tt.name); 
      return tt; 
    } 
    return null;
  }
}
