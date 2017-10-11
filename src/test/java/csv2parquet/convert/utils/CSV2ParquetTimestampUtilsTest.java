package csv2parquet.convert.utils;

import static org.junit.Assert.*;

import java.text.ParseException;

import org.apache.parquet.example.data.simple.NanoTime;
import org.junit.Test;

import jfseb.csv2parquet.convert.utils.CSV2ParquetTimestampUtils;

public class CSV2ParquetTimestampUtilsTest {

  @Test
  public void testFromDateTimeString() throws ParseException {
    String ts = "2017-01-02 00:13:45";
    NanoTime res = CSV2ParquetTimestampUtils.fromDateTimeString(ts);
    assertEquals(ts,CSV2ParquetTimestampUtils.binaryToDateTimeString(res.toBinary()));
    
  }
  @Test
  public void testFromMillis1() throws ParseException {
    String ts = "00:00:05.123";
    int res = CSV2ParquetTimestampUtils.parseTimeMillisInt(ts,  true); 
    assertEquals(ts,CSV2ParquetTimestampUtils.formatTimeMillis(res));
  }
  @Test
  public void testFromMillisStrict() {
    String ts = "00:00:05";
      int res;
      try {
        res = CSV2ParquetTimestampUtils.parseTimeMillisInt(ts,  true);
        fail("shoudl not get here");
      } catch (ParseException e) {
        // TODO Auto-generated catch block
        assertEquals(ts,ts);
      }   
  }
  
  @Test
  public void testFromMillisStrictFalse() throws ParseException {
    String ts = "00:00:05";
    int res = CSV2ParquetTimestampUtils.parseTimeMillisInt(ts,  false); 
    assertEquals(ts + ".000",CSV2ParquetTimestampUtils.formatTimeMillis(res));
  }
  
  @Test
  public void testFromMillis() throws ParseException {
    String ts = "00:13:45.123";
    int res = CSV2ParquetTimestampUtils.parseTimeMillisInt(ts, true);
    assertEquals(ts,CSV2ParquetTimestampUtils.formatTimeMillis(res));
  }
  
  public void testFromMicros() throws ParseException {
    String ts = "00:13:45.123";
    long res = CSV2ParquetTimestampUtils.parseTimeMicros(ts, true); 
    assertEquals(ts,CSV2ParquetTimestampUtils.formatTimeMicros(res));
  }
  
  @Test
  public void testFromDate() throws ParseException {
    String ts = "1970-01-02";
    int res = Integer.parseInt(CSV2ParquetTimestampUtils.parseDateOrInt(ts));
    assertEquals(ts,CSV2ParquetTimestampUtils.formatDate(res));
  }
  
  @Test
  public void testFromDateSAP() throws ParseException {
    String ts = "19700102";
    int res = Integer.parseInt(CSV2ParquetTimestampUtils.parseDateOrInt(ts));
    assertEquals("1970-01-02",CSV2ParquetTimestampUtils.formatDate(res));
  }
  @Test
  public void testFromDateSAP2() throws ParseException {
    String ts = "20170102";
    int res = Integer.parseInt(CSV2ParquetTimestampUtils.parseDateOrInt(ts));
    assertEquals("2017-01-02",CSV2ParquetTimestampUtils.formatDate(res));
  }
  
  @Test
  public void testFromDate2() throws ParseException {
    String ts = "2017-05-02";
    int res = Integer.parseInt(CSV2ParquetTimestampUtils.parseDateOrInt(ts));
    assertEquals(ts,CSV2ParquetTimestampUtils.formatDate(res));
  }
}
