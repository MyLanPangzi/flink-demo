package com.hiscat.flink.connctor.redis;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.DataType;

import java.util.List;

public class RedisDynamicTableSink implements DynamicTableSink {
    private final RedisOptions options;
    private final String pkName;
    private final List<String> columnNames;
    private final List<DataType> columnDataTypes;

    public RedisDynamicTableSink(final RedisOptions options,
                                 final String pkName, final List<String> columnNames, final List<DataType> columnDataTypes) {

        this.options = options;
        this.pkName = pkName;
        this.columnNames = columnNames;
        this.columnDataTypes = columnDataTypes;
    }

    @Override
    public ChangelogMode getChangelogMode(final ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(final Context context) {
        System.out.println("getSinkRuntimeProvider");
        return SinkFunctionProvider.of(new RedisSinkFunction(options, pkName, columnNames, columnDataTypes));
    }

    @Override
    public DynamicTableSink copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return null;
    }
}
