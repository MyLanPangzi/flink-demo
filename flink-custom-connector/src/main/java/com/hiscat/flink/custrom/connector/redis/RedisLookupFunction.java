package com.hiscat.flink.custrom.connector.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RedisLookupFunction extends TableFunction<Row> {

    private final RedisOptions options;
    private final List<String> columnNames;
    private Cache<String, Row> cache;
    private RedisCommands<String, String> commands;
    private StatefulRedisConnection<String, String> connection;

    public RedisLookupFunction(final RedisOptions options, final List<String> columnNames) {
        this.options = options;
        this.columnNames = columnNames;
    }

    @Override
    public void open(final FunctionContext context) {
        connection = ConnectionProvider.getConnection(options);
        commands = this.connection.sync();

        this.cache =
            options.getCacheSize() == -1 || options.getCacheExpireMs() == -1
                ? null
                : CacheBuilder.newBuilder()
                .expireAfterWrite(options.getCacheExpireMs(), TimeUnit.MILLISECONDS)
                .maximumSize(options.getCacheSize())
                .build();

    }

    @SuppressWarnings("unused")
    public void eval(String pk) {
        final String redisKey = String.format("%s%s%s", options.getTableName(), options.getDelimiter(), pk);
        if (cache != null) {
            Row cachedRows = cache.getIfPresent(redisKey);
            if (cachedRows != null && cachedRows.getArity() > 0) {
                collect(cachedRows);
                return;
            }
        }


        for (int retry = 0; retry <= options.getMaxRetryTimes(); retry++) {
            try {
                final Map<String, String> map = commands.hgetall(redisKey);
                final Row row = Row.withNames();
                if (!map.isEmpty()) {
                    columnNames.forEach(c -> row.setField(c, map.get(c)));
                    row.setField(options.getPrimaryKey(), pk);
                    collect(row);
                }
                if (cache != null) {
                    cache.put(redisKey, row);
                }
                break;
            } catch (Exception e) {
                log.error("lookup redis error : {}", e.getMessage(), e);
                try {
                    //noinspection BusyWait
                    Thread.sleep(1000L*retry);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        }
    }

    @Override
    public void close() {
        connection.close();
    }
}
