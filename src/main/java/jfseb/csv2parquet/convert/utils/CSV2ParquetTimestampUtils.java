package jfseb.csv2parquet.convert.utils;

import java.sql.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.TimeZone;

import javax.print.attribute.standard.DateTimeAtCompleted;

import org.apache.parquet.example.data.simple.Int96Value;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.io.api.Binary;

public class CSV2ParquetTimestampUtils {

  public static NanoTime fromDateTimeString(String val) throws ParseException {

    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // , Locale.ENGLISH);
    java.util.Date result = df.parse(val);
    // try a byte array a la
    // https://www.programcreek.com/java-api-examples/index.php?source_dir=presto-master/presto-hive/src/test/java/com/facebook/presto/hive/parquet/TestParquetTimestampUtils.java
    // todo : parse millis
    long unixSecs = result.getTime();
    int julianDay = getJulianDaysFromUnix(unixSecs);
    long timeOfDayNanos = getJulianTimeInNanosFromUnix(unixSecs);

    return new NanoTime(julianDay, timeOfDayNanos);
  }

  static final double OFFSETJULIAN = 2440587.5;

  static double getJulianFromUnix(double unixSecs) {
    return (unixSecs / 86400.0) + OFFSETJULIAN;
  }

  static int getJulianDaysFromUnix(long unixSecs) {
    return Double.valueOf(getJulianFromUnix(unixSecs)).intValue();
  }

  static long getJulianTimeInNanosFromUnix(long unixSecs) {
    long remainder = (unixSecs + 86400 / 2) % 86400;
    return remainder * 1000;
  }

  static long getJulianTimeInNanosFromUnix2(long unixSecs) {
    double frac = getJulianFromUnix(unixSecs) - getJulianDaysFromUnix(unixSecs);
    frac *= 86400.0 * 1000;
    return Double.valueOf(frac).longValue();
  }

  public static String binaryToDateTimeString(Binary int96b) {
    NanoTime nt = NanoTime.fromBinary(int96b);
    int day = nt.getJulianDay();
    long nanos = nt.getTimeOfDayNanos();
    long unixTime = Double.valueOf((day - 2440587.5) * 86400.0 + nanos / 1000.0).longValue();
    Date dt = new Date(unixTime);
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // , Locale.ENGLISH);
    return df.format(dt);
  }

  public static String parseDateOrInt(String val) {
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd"); // , Locale.ENGLISH);
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    java.util.Date result;
    try {
      result = df.parse(val);
      return "" + Double.valueOf(result.getTime() / (24l * 60 * 60 * 1000)).intValue();
    } catch (ParseException e) {
      if (false && val != null && (val.length() == "yyyyMMdd".length())) {
        DateFormat df2 = new SimpleDateFormat("yyyyMMdd"); // , Locale.ENGLISH);
        df2.setTimeZone(TimeZone.getTimeZone("UTC"));
        try {
          result = df2.parse(val);
          return "" + Double.valueOf(result.getTime() / (24l * 60 * 60 * 1000)).intValue();
        } catch (ParseException e2) {
          return "" + Long.parseLong(val);          
        }
      } else {
        return "" + Long.parseLong(val);          
      }
    }
  }

  public static String formatDate(int value) {
    long milliseconds = (long) value * (24l * 60 * 60 * 1000);
    java.util.Date dt = Date.from(Instant.ofEpochMilli(milliseconds));

    DateFormat sd = new SimpleDateFormat("yyyy-MM-dd");
    sd.setTimeZone(TimeZone.getTimeZone("UTC"));
    return sd.format(dt);
  }

  public static String formatTimeMicros(long value) {
    long milliseconds = Double.valueOf(value / 1000000).longValue() * 1000;
    SimpleDateFormat sd = new SimpleDateFormat("HH:mm:ss.");
    sd.setTimeZone(TimeZone.getTimeZone("UTC"));
    return sd.format(milliseconds) + String.format("%06d", value % 1000000);
  }

  public static String formatTimeMillis(int value) {
    SimpleDateFormat sd = new SimpleDateFormat("HH:mm:ss.SSS");
    sd.setTimeZone(TimeZone.getTimeZone("UTC"));
    return sd.format(Date.from(Instant.ofEpochMilli(value)));

  }

  public static long parseTimeMicros(String val, boolean strict) throws ParseException {
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss."); // , Locale.ENGLISH);
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    java.util.Date result;
    try {
      String dateval = "1970-01-01 " + val;
      ParsePosition pp = new ParsePosition(0);
      result = df.parse(dateval, pp);
      if (pp.getErrorIndex() >= 0) {
        throw new ParseException("error parsing" + val, pp.getErrorIndex());
      }
      int index = pp.getIndex();
      int micros = Integer.parseInt(dateval.substring(pp.getIndex()));
      return (result.getTime() * 1000l + micros);
    } catch (ParseException e) {
      if (strict) {
        throw e;
      }
      return parseTimeMicrosSloppy(val);
    }
  }

  public static int parseTimeMillisOrInt(String val) {
    try {
      return parseTimeMillisInt(val, false);
    } catch (ParseException e) {
      return Integer.parseInt(val);
    }
  }

  public static int parseTimeMicrosSloppy(String val) throws ParseException {
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // , Locale.ENGLISH);
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    java.util.Date result;
    result = df.parse("1970-01-01 " + val);
    return (int) result.getTime() * 1000;
  }

  public static int parseTimeMillisInt(String val, boolean b) throws ParseException {
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"); // , Locale.ENGLISH);
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    java.util.Date result;
    try {
      result = df.parse("1970-01-01 " + val);
      return (int) result.getTime();
    } catch (ParseException e) {
      if (b)
        throw e;
    }
    long parseTimeMicros = parseTimeMicrosSloppy(val);
    return (int) parseTimeMicros / 1000;
  }
}
