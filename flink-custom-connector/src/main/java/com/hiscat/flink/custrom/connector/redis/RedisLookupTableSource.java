package com.hiscat.flink.custrom.connector.redis;

import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;

import java.util.List;

public class RedisLookupTableSource implements LookupTableSource {

    private final RedisOptions options;
    private final List<String> columnNames;

    public RedisLookupTableSource(final RedisOptions options, final List<String> columnNames) {
        this.options = options;
        this.columnNames = columnNames;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(final LookupContext context) {
        if (options.getAsync()) {
            return AsyncTableFunctionProvider.of(new RedisAsyncLookupFunction(options, columnNames));
        }
        return TableFunctionProvider.of(new RedisLookupFunction(options, columnNames));
    }

    @Override
    public DynamicTableSource copy() {
        return new RedisLookupTableSource(options, columnNames);
    }

    @Override
    public String asSummaryString() {
        return "redis dynamic table";
    }
}
