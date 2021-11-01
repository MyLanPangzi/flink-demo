package com.hiscat.flink.connctor.redis;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.Set;

import static com.hiscat.flink.connctor.redis.RedisOptions.*;

public class RedisDynamicTableSinkFactory implements DynamicTableSinkFactory {
    @Override
    public DynamicTableSink createDynamicTableSink(final Context context) {
        System.out.println("createDynamicTableSink");
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        helper.validate();
        final RedisOptions options = RedisOptions.builder()
            .host(helper.getOptions().get(HORT))
            .port(helper.getOptions().get(PORT))
            .keyPrefix(helper.getOptions().get(SINK_KEY_PREFIX))
            .build();

        final ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        final String pkName = schema.getPrimaryKey().orElseThrow(() -> new RuntimeException("missing primary key"))
            .getColumns().get(0);
        final List<String> columnNames = schema.getColumnNames();
        final List<DataType> columnDataTypes = schema.getColumnDataTypes();

        return new RedisDynamicTableSink(options, pkName, columnNames, columnDataTypes);
    }

    @Override
    public String factoryIdentifier() {
        return "redis";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Sets.newHashSet(HORT);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Sets.newHashSet(PORT, SINK_KEY_PREFIX);
    }
}
