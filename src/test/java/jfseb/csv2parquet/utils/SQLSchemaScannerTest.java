package jfseb.csv2parquet.utils;

import static org.junit.Assert.*;

import org.apache.orc.OrcProto.Type;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.junit.Test;

import jfseb.csv2parquet.utils.SQLSchemaScanner.SQLType;

public class SQLSchemaScannerTest {

  @Test
  public void testExtractSQLType() {
    SQLType tt = SQLSchemaScanner.extractSQLType("ABC CHARACTER VARYING(16)");
    assertEquals(tt.name ,"ABC"); 
    assertEquals(tt.length, 16);
    assertEquals(tt.primitiveType.getName(),"ABC");
  }
  @Test
  public void testExtractSQLTypeVARCHAR() {
    SQLType tt = SQLSchemaScanner.extractSQLType("abc VARcHAR(37) NULL");
    assertEquals(tt.name ,"abc"); 
    assertEquals(tt.length, 37);
    assertEquals(tt.primitiveType.getRepetition(), Repetition.OPTIONAL);
    assertEquals(tt.primitiveType.getName(),"abc");
  }
  @Test
  public void testExtractSQLTypeINT() {
    SQLType tt = SQLSchemaScanner.extractSQLType("abc INT NULL");
    assertEquals(tt.name ,"abc"); 
    assertEquals(0, tt.length);
    assertEquals(tt.primitiveType.getRepetition(), Repetition.OPTIONAL);
    assertEquals(tt.primitiveType.getOriginalType(), OriginalType.INT_64); 
  }
  @Test
  public void testExtractSQLTypeDOUBLE() {
    SQLType tt = SQLSchemaScanner.extractSQLType("abc DOUBLE NULL");
    assertEquals(tt.name ,"abc"); 
    assertEquals(0, tt.length);
    assertEquals(tt.primitiveType.getRepetition(), Repetition.OPTIONAL);
    assertEquals(tt.primitiveType.getPrimitiveTypeName(), PrimitiveTypeName.DOUBLE);
  } 
  @Test
  public void testExtractSQLTypedDATE() {
    SQLType tt = SQLSchemaScanner.extractSQLType("abc DATE NULL");
    assertEquals(tt.name ,"abc"); 
    assertEquals(0, tt.length);
    assertEquals(tt.primitiveType.getRepetition(), Repetition.OPTIONAL);
    assertEquals(tt.primitiveType.getOriginalType(), OriginalType.DATE); 
    assertEquals(tt.primitiveType.getPrimitiveTypeName(), PrimitiveTypeName.INT64);
  } 
  @Test
  public void testExtractSQLTypedTIME() {
    SQLType tt = SQLSchemaScanner.extractSQLType("abc TIME NULL");
    assertEquals(tt.name ,"abc"); 
    assertEquals(tt.length, 0);
    assertEquals(tt.primitiveType.getRepetition(), Repetition.OPTIONAL);
    assertEquals(tt.primitiveType.getOriginalType(), OriginalType.TIME_MILLIS); 
    assertEquals(tt.primitiveType.getPrimitiveTypeName(), PrimitiveTypeName.INT64);
  } 
  @Test
  public void testExtractSQLTypeSHORTINT() {
    SQLType tt = SQLSchemaScanner.extractSQLType("abc SHORTINT NULL");
    assertEquals(tt.name ,"abc"); 
    assertEquals(tt.length, 0);
    assertEquals(tt.primitiveType.getRepetition(), Repetition.OPTIONAL);
    assertEquals(tt.primitiveType.getPrimitiveTypeName(), PrimitiveTypeName.INT32);
    assertEquals(tt.primitiveType.getOriginalType(), OriginalType.INT_32); 
  }

  @Test
  public void testExtractSQLTypeTIMESTAMP() {
    SQLType tt = SQLSchemaScanner.extractSQLType("abc TIMESTAMP NULL");
    assertEquals(tt.name ,"abc"); 
    assertEquals(tt.length, 0);
    assertEquals(tt.primitiveType.getRepetition(), Repetition.OPTIONAL);
    assertEquals(tt.primitiveType.getOriginalType(), OriginalType.TIMESTAMP_MICROS); 
  }
}
