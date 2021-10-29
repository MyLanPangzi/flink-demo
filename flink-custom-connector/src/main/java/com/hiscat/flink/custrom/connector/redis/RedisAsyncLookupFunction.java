package com.hiscat.flink.custrom.connector.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RedisAsyncLookupFunction extends AsyncTableFunction<Row> {


    private final RedisOptions options;
    private final List<String> columnNames;
    private Cache<String, Row> cache;
    private RedisAsyncCommands<String, String> commands;
    private StatefulRedisConnection<String, String> connection;

    public RedisAsyncLookupFunction(final RedisOptions options, final List<String> columnNames) {
        this.options = options;
        this.columnNames = columnNames;
    }

    @Override
    public void open(final FunctionContext context) {
        connection = ConnectionProvider.getConnection(options);
        commands = this.connection.async();

        this.cache =
            options.getCacheSize() == -1 || options.getCacheExpireMs() == -1
                ? null
                : CacheBuilder.newBuilder()
                .expireAfterWrite(options.getCacheExpireMs(), TimeUnit.MILLISECONDS)
                .maximumSize(options.getCacheSize())
                .build();
    }

    @SuppressWarnings("unused")
    public void eval(CompletableFuture<Collection<Row>> future, String pk) {
        int currentRetry = 0;
        if (cache != null) {
            Row cacheRow = cache.getIfPresent(pk);
            if (cacheRow != null) {
                if (cacheRow.getArity() == 0) {
                    future.complete(Collections.emptyList());
                } else {
                    future.complete(Collections.singletonList(cacheRow));
                }
                return;
            }
        }
        fetchResult(future, currentRetry, pk);
    }

    private void fetchResult(
        CompletableFuture<Collection<Row>> resultFuture, int currentRetry, String pk) {
        commands.hgetall(String.format("%s%s%s", options.getTableName(), options.getDelimiter(), pk))
            .whenCompleteAsync(
                (result, throwable) -> {
                    if (throwable != null) {
                        log.error(String.format("Redis asyncLookup error, retry times = %d", currentRetry), throwable);
                        if (currentRetry >= options.getMaxRetryTimes()) {
                            resultFuture.completeExceptionally(throwable);
                            return;
                        }
                        try {
                            Thread.sleep(1000L*currentRetry);
                        } catch (InterruptedException e1) {
                            resultFuture.completeExceptionally(e1);
                            return;
                        }
                        fetchResult(resultFuture, currentRetry + 1, pk);
                        return;
                    }
                    final Row row = Row.withNames();
                    if (!result.isEmpty()) {
                        columnNames.forEach(c -> row.setField(c, result.get(c)));
                        row.setField(options.getPrimaryKey(), pk);
                        resultFuture.complete(Collections.singletonList(row));
                    } else {
                        resultFuture.complete(Collections.emptyList());
                    }
                    if (cache != null) {
                        cache.put(pk, row);
                    }
                });
    }

    @Override
    public void close() {
        connection.close();
    }
}
