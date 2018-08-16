package jfseb.csv2parquet.utils;

import static org.junit.Assert.*;

import org.apache.orc.OrcProto.Type;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.junit.Test;

import jfseb.csv2parquet.utils.ParseHexRec.ParsedRec;
import jfseb.csv2parquet.utils.SQLSchemaScanner.SQLType;

public class ParseHexRecTest {

  @Test
  public void testLen() {
    assertEquals("ABC", ParseHexRec.getLen("ABC", 3));
    assertEquals("BC", ParseHexRec.getLen("ABC", 2));
    assertEquals("0ABC", ParseHexRec.getLen("ABC", 4));
    assertEquals("000ABC", ParseHexRec.getLen("ABC", 6));
  }

  @Test
  public void testNoParse() {
    ParsedRec res = ParseHexRec.parse("0x02Zx0");
    assertEquals(ParseHexRec.parse("0x02Zx0"), null);
    assertEquals(ParseHexRec.parse("0xAF"), null);
    assertEquals(ParseHexRec.parse("0xx0"), null);
    assertEquals(ParseHexRec.parse("0xAx0"), null);
    assertEquals(ParseHexRec.parse("0xUAx0"), null);
  }

  @Test
  public void testParse() {
    ParsedRec res = ParseHexRec.parse("0x02x0");
    assertEquals(res.asBool, false);
    assertEquals(res.asInt, 2);
    assertEquals(res.asLong, 2);
    assertEquals(res.binary.getBytes().length, 1);
  }

  @Test
  public void testExtractString() {
    ParsedRec res = ParseHexRec.parse("0x65x0");
    assertEquals(res.asBool, false);
    assertEquals(res.asInt, 0x65);
    assertEquals(res.asLong, 0x65);
    assertEquals(res.getBinary(96 / 8).length(), 96 / 8);
    assertEquals(res.getBinary(14).getBytes()[13], 0x65);
  }

  @Test
  public void testParseNegInt() {

    ParsedRec res = ParseHexRec.parse("0xFFF0FE01x0");
    assertTrue(Float.isNaN(res.asFloat));
    assertTrue(Double.isFinite(res.asDouble));
    assertEquals(res.asBool, false);
    assertEquals(res.asInt, -983551);
    assertEquals(res.asLong, 4293983745l);
    assertEquals(res.binary.getBytes().length, 4);
    assertEquals(res.binary.getBytes()[0], -1);
    assertEquals(res.binary.getBytes()[1], (byte) 0xF0);
    assertEquals(res.binary.getBytes()[2], -2);
    assertEquals(res.binary.getBytes()[3], 0x01);
    assertTrue(Float.isNaN(res.asFloat));
    assertTrue(Double.isFinite(res.asDouble));
  }
}
