package com.hiscat.flink.connctor.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RedisSinkFunction extends RichSinkFunction<RowData> {
    private final Map<String, RowData.FieldGetter> getters;
    private StatefulRedisConnection<String, String> connection;

    private RedisCommands<String, String> commands;
    private final RedisOptions options;
    private final String pkName;
    private final List<String> columnNames;


    public RedisSinkFunction(final RedisOptions options,
                             final String pkName,
                             final List<String> columnNames,
                             final List<DataType> columnDataTypes) {
        this.options = options;
        this.pkName = pkName;
        this.columnNames = columnNames;
        getters = columnNames
            .stream()
            .map(c -> {
                final int index = columnNames.indexOf(c);
                return Tuple2.of(c, RowData.createFieldGetter(columnDataTypes.get(index).getLogicalType(), index));
            })
            .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    }

    @Override
    public void open(final Configuration parameters) throws Exception {
        super.open(parameters);
        RedisClient client = RedisClient.create(String.format("redis://%s:%s", options.getHost(), options.getPort()));
        connection = client.connect();
        commands = connection.sync();
    }

    @Override
    public void invoke(final RowData value, final Context context) {
        System.out.println(value);
        final Map<String, String> map = new HashMap<>();
        getters.forEach((k, v) -> {
            final Object fieldOrNull = v.getFieldOrNull(value);
            if (fieldOrNull != null) {
                map.put(k, fieldOrNull.toString());
            }
        });
        commands.hset(String.format("%s:%s", options.getKeyPrefix(), value.getString(columnNames.indexOf(pkName))), map);
    }

    @Override
    public void close() {
        connection.close();
    }
}
