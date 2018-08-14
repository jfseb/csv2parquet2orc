package jfseb.csv2parquet.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;

import sun.tools.tree.BinaryArithmeticExpression;


/**
 * Scans simle SQL schemas
 * @author
 *
 */
public class ParseHexRec {

 
  static public class ParsedRec {
    ParsedRec(){
      asLong = 0;
      asFloat = 0;
      asDouble = 0;
      asInt = 0;
      asBool = false;
    }
    public Binary binary;
    public long asLong;
    public float asFloat;
    public double asDouble;
    public int asInt;
    public boolean asBool;
    public String asString;
  }
  
  /**
   * returns sring truncated to len, padded to the left with 0 if required
   * @param val
   * @param len
   */
  public static String getLen(String val, int len) {
    String res = val;
    if(res.length() > len) {
      res = res.substring(res.length()-len);
    }
    while(res.length() < len) {
      res = '0' + res;
    }
    return res;
  }

  public static ParsedRec parse(String val) {
    // content is supposed to be hexadecimal, 
    // 
    if( !val.matches("[A-F0-9]+")) 
    {
      throw new IllegalStateException(" string " + val + " is node made up of [A-F0-9]");
    }
    ParsedRec res = new ParsedRec();
    String len16 = getLen(val, 16); 
    String len8 = getLen(val, 8); 
    res.asLong = Long.parseUnsignedLong(len16, 16);
    res.asInt = Integer.parseUnsignedInt(len8, 16);
    res.asFloat = Float.intBitsToFloat(res.asInt);
    res.asDouble = Double.longBitsToDouble(res.asLong);
    if( val.length() % 2 != 0) {
      throw new IllegalArgumentException(" odd number of hex digits in " + val );
    }
    byte[] bytes = new byte[val.length()/2];
    for(int i = 0; i < val.length() / 2; ++i) 
    {
      String v = new StringBuilder().append(val.charAt(i*2)).append(val.charAt(i*2+1)).toString();
      int u = Integer.parseUnsignedInt(v,16);
      bytes[i] = (byte) u; 
    }
    res.binary = Binary.fromConstantByteArray(bytes);
    return res;
  }
   
  static private Binary stringToBinary(String value) {
    return Binary.fromString(value);
  }
}
