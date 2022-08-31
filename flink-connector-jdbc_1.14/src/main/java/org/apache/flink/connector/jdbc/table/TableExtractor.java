package org.apache.flink.connector.jdbc.table;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.function.Function;

public class TableExtractor implements Function<RowData, String >, Serializable {
    private static final long serialVersionUID = 1L;

    private final int pos;

    public TableExtractor(int pos) {
        this.pos = pos;
    }

    public int getPos() {
        return pos;
    }

    @Override
    public String apply(RowData rowData) {
        return RouteJdbcDynamicTableSink.TableMetadata.TABLE.converter.read(rowData, pos).toString();
    }
}
