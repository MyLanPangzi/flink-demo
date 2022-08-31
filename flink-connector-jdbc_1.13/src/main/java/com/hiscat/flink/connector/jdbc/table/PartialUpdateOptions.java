package com.hiscat.flink.connector.jdbc.table;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.RowData;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class PartialUpdateOptions implements Serializable {
  private static final long serialVersionUID = 1L;

  private String tag;
  private List<String> fields;
  private JdbcRowConverter converter;
  private transient List<RowData> buffer;
  private transient FieldNamedPreparedStatement preparedStatement;

  public void close() {
    try {
      preparedStatement.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void flush() {
    buffer.forEach(
        e -> {
          try {
            converter.toExternal(e, preparedStatement);
            preparedStatement.addBatch();
          } catch (SQLException ex) {
            throw new RuntimeException(ex);
          }
        });
    try {
      preparedStatement.executeBatch();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    buffer.clear();
  }
}
