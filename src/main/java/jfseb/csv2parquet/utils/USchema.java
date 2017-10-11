package jfseb.csv2parquet.utils;

import java.util.List;

import org.apache.orc.TypeDescription;
import org.apache.parquet.schema.MessageType;

import jfseb.csv2parquet.utils.SQLSchemaScanner.SQLType;

public class USchema {

  public MessageType messageType;
  public TypeDescription typeDescription;
  public List<SQLType> sqlTypes;

}
