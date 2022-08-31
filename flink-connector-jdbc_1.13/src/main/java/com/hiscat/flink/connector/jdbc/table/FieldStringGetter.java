package com.hiscat.flink.connector.jdbc.table;

import java.time.format.DateTimeFormatter;
import javax.annotation.Nullable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;

public final class FieldStringGetter implements FieldGetter {
  private static final long serialVersionUID = 1L;

  private final LogicalType logicalType;
  private final FieldGetter fieldGetter;

  public FieldStringGetter(LogicalType logicalType, FieldGetter fieldGetter) {
    this.logicalType = logicalType;
    this.fieldGetter = fieldGetter;
  }

  @Nullable
  @Override
  public Object getFieldOrNull(RowData row) {
    Object data = fieldGetter.getFieldOrNull(row);
    if (data == null) {
      return "null";
    }
    switch (this.logicalType.getTypeRoot()) {
      case CHAR:
      case VARCHAR:
        return String.format("'%s'", escape(data.toString()));

      case BINARY:
      case VARBINARY:
        return String.format("'%s'", escape(new String((byte[]) data)));

      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        TimestampData timestampData = (TimestampData) data;
        return String.format(
            "'%s'",
            timestampData
                .toLocalDateTime()
                .format(DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss.SSS")));

      case DATE:
      case TIMESTAMP_WITH_TIME_ZONE:
      case TIME_WITHOUT_TIME_ZONE:
      case INTERVAL_YEAR_MONTH:
      case ARRAY:
      case MULTISET:
      case MAP:
      case ROW:
      case STRUCTURED_TYPE:
      case DISTINCT_TYPE:
      case RAW:
      case NULL:
      case SYMBOL:
      case UNRESOLVED:
      case BOOLEAN:
      case DECIMAL:
      case FLOAT:
      case DOUBLE:
      case BIGINT:
      case INTEGER:
      case SMALLINT:
      case TINYINT:
      case INTERVAL_DAY_TIME:
        return String.valueOf(data);
      default:
        throw new IllegalArgumentException();
    }
  }

  private String escape(String result) {
    return result.replace("'", "").replace("\\", "");
  }
}
