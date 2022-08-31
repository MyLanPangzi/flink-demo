package com.hiscat.flink.connector.jdbc.table;

import java.io.Serializable;
import java.util.function.Function;
import org.apache.flink.table.data.RowData;

public class PrepareStatementTagExtractor implements Function<RowData, String>, Serializable {

  private final int pos;

  public PrepareStatementTagExtractor(int pos) {
    this.pos = pos;
  }

  @Override
  public String apply(RowData rowData) {
    return PartialUpdateJdbcDynamicTableSink.PrepareStatementTagMetadata.PREPARE_STATEMENT_TAG
        .converter
        .read(rowData, pos)
        .toString();
  }

  public int getPos() {
    return pos;
  }
}
