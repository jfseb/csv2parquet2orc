package jfseb.csv2parquet.utils;

import java.io.File;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.orc.TypeDescription;
import org.apache.orc.TypeDescription.Category;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.TypeConverter;
import org.apache.parquet.schema.TypeVisitor;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.ID;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.tools.Main;

import jfseb.csv2parquet.TypeMapping;
import jfseb.csv2parquet.utils.SQLSchemaScanner.SQLType;
import jfseb.csv2parquet.utils.USchema;

public class SchemaCreator {

  public static USchema makeSchema(File csvFile, String schemaStr) {
    // attempt to parse a schema from schemastr.

    USchema res = new USchema();
    if (schemaStr != null && schemaStr.indexOf("struct<") == 0) {
      // assuming an orc schema
      // cleanse all string.
      schemaStr = schemaStr.replaceAll("\\n", "");
      schemaStr = schemaStr.replaceAll("\\r", "");
      schemaStr = schemaStr.replaceAll("\\s", "");
      res.typeDescription = TypeDescription.fromString(schemaStr);
    } else if (schemaStr != null && schemaStr.indexOf("message") >= 0 && schemaStr.indexOf("{") >= 0) {
      // assuming an parquet schema
      res.messageType = MessageTypeParser.parseMessageType(schemaStr);
      res.typeDescription = makeTypeDescriptionFrom(res.messageType);

    } else if (schemaStr != null && schemaStr.indexOf("TABLE") >= 0 && schemaStr.indexOf("CREATE") >= 0) {
      String[] lines = schemaStr.split("\n");
      ArrayList<Type> fields = new ArrayList<Type>();
      List<SQLType> sqlTypes = new ArrayList<SQLType>();
      for (int i = 0; i < lines.length; ++i) {
        String line = lines[i];
        SQLType pt = SQLSchemaScanner.extractSQLType(line);
        if (pt != null) {
          fields.add(pt.primitiveType);
          sqlTypes.add(pt);
        }
      }
      res.messageType = new MessageType("m", fields);
      res.sqlTypes = sqlTypes;
      res.typeDescription = makeTypeDescriptionFrom(res.messageType, res.sqlTypes);
    }
    if (res.messageType != null && res.typeDescription == null) {
      res.typeDescription = makeTypeDescriptionFrom(res.messageType);
    }
    if (res.messageType == null && res.typeDescription != null) {
      res.messageType = makeTypeDescriptionFrom(res.typeDescription);
    }
    return res;
  }

  private static TypeDescription makeTypeDescriptionFrom(MessageType messageType) {
    return makeTypeDescriptionFrom(messageType, null);
  }

  private static TypeDescription makeTypeDescriptionFrom(MessageType messageType, List<SQLType> sqlTypes) {
    TypeDescription result = new TypeDescription(Category.STRUCT);
    int i = 0;
    for (Iterator<Type> it = messageType.getFields().listIterator(); it.hasNext();) {
      Type tp = it.next();
      SQLType sqltype = (sqlTypes == null) ? null : sqlTypes.get(i);
      TypeDescription td = makeTypeDescription(tp, sqltype);
      result.addField(tp.getName(), td);
      i = i + 1;
    }
    return result;
  }

  private static MessageType makeTypeDescriptionFrom(TypeDescription td) {
    List<Type> fields = new ArrayList<Type>();
    for (int i = 0; i < td.getChildren().size(); ++i) {
      TypeDescription tdf = td.getChildren().get(i);
      String name = td.getFieldNames().get(i);
      PrimitiveType prim = makePrimitiveType(name, tdf);
      // boolean Null = TypeMapping.getNull(tp);
      // Main.out.println(tp.getName() + " " + TypeMapping.getSqlType(tp) + (Null ? "
      // NULL" : "") + ";"
      // + ((auxType != null) ? "-- " + auxType : ""));
      fields.add(prim);
    }
    return new MessageType("m", fields);
  }

  private static PrimitiveType makePrimitiveType(String name, TypeDescription tdf) {
    Repetition rep = TypeMapping.getNull(tdf) ? Repetition.OPTIONAL : Repetition.REQUIRED;
    switch (tdf.getCategory()) {
    case BOOLEAN:
      return new PrimitiveType(rep, PrimitiveTypeName.BOOLEAN, name);
    case BINARY:
      return new PrimitiveType(rep, PrimitiveTypeName.BINARY, name);
    case VARCHAR:
      return new PrimitiveType(rep, PrimitiveTypeName.BINARY, name);
    case DECIMAL:
      return new PrimitiveType(rep, PrimitiveTypeName.BINARY, name);
    case DOUBLE:
      return new PrimitiveType(rep, PrimitiveTypeName.DOUBLE, name);
    case FLOAT:
      return new PrimitiveType(rep, PrimitiveTypeName.FLOAT, name);
    case INT:
      return new PrimitiveType(rep, PrimitiveTypeName.INT32, name);
    case LONG:
      return new PrimitiveType(rep, PrimitiveTypeName.INT64, name);
    case TIMESTAMP:
      return new PrimitiveType(rep, PrimitiveTypeName.INT96, name);
    default:
      throw new IllegalArgumentException(tdf.toString() + "\n" + tdf.toJson());
    }
  }

  private static TypeDescription makeTypeDescription(Type tp, SQLType sqlType) {
    PrimitiveType tprim = tp.asPrimitiveType();
    PrimitiveTypeName nmprim = tprim.getPrimitiveTypeName();
    if (PrimitiveTypeName.BINARY == nmprim) {
      if (tp.getOriginalType() == OriginalType.DECIMAL) {
        return TypeDescription.createDecimal();
      }
      if (tp.getOriginalType() == OriginalType.DECIMAL) {
        return TypeDescription.createDecimal();
      }
      return TypeDescription.createVarchar();
    } else if (PrimitiveTypeName.INT32 == nmprim) {
      if (tp.getOriginalType() == OriginalType.DATE) {
        return TypeDescription.createDate();
      }
      if (tp.getOriginalType() == OriginalType.TIME_MILLIS) {
        return TypeDescription.createTimestamp();
      }
      if (tp.getOriginalType() == OriginalType.TIME_MICROS) {
        return TypeDescription.createTimestamp();
      }
      return TypeDescription.createInt();
    } else if (PrimitiveTypeName.INT64 == nmprim) {
      if (tp.getOriginalType() == OriginalType.DATE) {
        return TypeDescription.createDate();
      }
      if (tp.getOriginalType() == OriginalType.TIME_MILLIS) {
        return TypeDescription.createTimestamp();
      }
      if (tp.getOriginalType() == OriginalType.TIME_MICROS) {
        return TypeDescription.createTimestamp();
      }
      return TypeDescription.createLong();
    } else if (PrimitiveTypeName.INT96 == nmprim) {
      return TypeDescription.createTimestamp();
    } else if (PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY == nmprim) {
      return TypeDescription.createBinary();
    } else if (PrimitiveTypeName.FLOAT == nmprim) {
      return TypeDescription.createFloat();
    } else if (PrimitiveTypeName.DOUBLE == nmprim) {
      return TypeDescription.createDouble();
    } else if (PrimitiveTypeName.BOOLEAN == nmprim ) {
      return TypeDescription.createBoolean();
    }
    throw new IllegalArgumentException("unknown type " + tp.getName() + "\n" + tp);
  }
}
