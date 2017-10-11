package jfseb.csv2parquet;

import org.apache.orc.TypeDescription;
import org.apache.orc.TypeDescription.Category;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.ID;

public class TypeMapping {

  public static String getSqlType(Type ftype) {
    String nm = ftype.getName();
    ID id = ftype.getId();
    OriginalType OT = ftype.getOriginalType();
    PrimitiveType tp = ftype.asPrimitiveType();
    PrimitiveTypeName nmprim = tp.getPrimitiveTypeName();
    if (PrimitiveTypeName.BINARY == nmprim) {
      return "VARCHAR";
    } else if (PrimitiveTypeName.INT32 == nmprim) {
      return "SMALLINT";
    } else if (PrimitiveTypeName.INT64 == nmprim) {
      return "BIGINT";
    } else if (PrimitiveTypeName.INT96 == nmprim) {
      return "TIMESTAMP";
    } else if (PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY == nmprim) {
      return "FIXED_LEN_BYTEARRAY";
    } else if (PrimitiveTypeName.FLOAT == nmprim) {
      return "FLOAT";
    } else if (PrimitiveTypeName.DOUBLE == nmprim) {
      return "DOUBLE";
    }
    throw new IllegalArgumentException("unknown type " + tp.getName() + "\n" + tp);
  }

  public static String getAuxSQLType(Type tp) {
    return null;
  }

  public static boolean getNull(Type tp) {
    if (tp.getRepetition() == Type.Repetition.OPTIONAL) {
      return true;
    }
    return false;
  }

  public static String getSqlType(TypeDescription ftype) {
    Category nmprim = ftype.getCategory();
    if (Category.BINARY == nmprim) {
      return "BINARY";
    } else if (Category.VARCHAR == nmprim) {
      return "VARCHAR(" + ftype.getMaxLength() + ")";
    } else if (Category.INT == nmprim) {
      return "INTEGER";
    } else if (Category.SHORT == nmprim) {
      return "SMALLINT";
    } else if (Category.STRING == nmprim) {
      return "STRING";
    } else if (Category.LONG == nmprim) {
      return "BIGINT";
    } else if (Category.DECIMAL == nmprim) {
      return "DECIMAL(" + ftype.getMaxLength() + "," + ftype.getPrecision() + ")";
    } else if (Category.DATE == nmprim) {
      return "DATE";
    } else if (Category.FLOAT == nmprim) {
      return "FLOAT";
    } else if (Category.DOUBLE == nmprim) {
      return "DOUBLE";
    } else {
      return nmprim.toString();
    }
    // throw new IllegalArgumentException("unknown type " + ftype.getCategory() + "\n" + ftype);
  }

  public static String getAuxSQLType(TypeDescription tp) {
    // TODO Auto-generated method stub
    return null;
  }

  public static boolean getNull(TypeDescription tp) {
    return true;
  }
}
