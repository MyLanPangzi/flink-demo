package com.hiscat.flink.custrom.connector.redis;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.hiscat.flink.custrom.connector.redis.RedisOptions.*;


public class RedisDynamicTableFactory implements DynamicTableSourceFactory {

    @Override
    public DynamicTableSource createDynamicTableSource(final Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        helper.validate();

        final ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        final RedisOptions redisOptions = RedisOptions.builder()
            .host(helper.getOptions().get(HOST))
            .port(helper.getOptions().get(PORT))
            .password(helper.getOptions().get(PASSWORD))
            .async(helper.getOptions().get(ASYNC))
            .cacheSize(helper.getOptions().get(CACHE_SIZE))
            .cacheExpireMs(helper.getOptions().get(CACHE_EXPIRE))
            .delimiter(helper.getOptions().get(DELIMITER))
            .maxRetryTimes(helper.getOptions().get(MAX_RETRY_TIMES))
            .primaryKey(helper.getOptions().getOptional(PRIMARY_KEY)
                .orElse(schema.getPrimaryKey()
                    .orElseThrow(() -> new NoSuchElementException("missing primary key")).getColumns().get(0)))
            .tableName(helper.getOptions().getOptional(TABLE_NAME).orElse(context.getObjectIdentifier().getObjectName()))
            .build();

        return new RedisLookupTableSource(redisOptions, schema.getColumnNames());
    }

    @Override
    public String factoryIdentifier() {
        return "iaa-redis";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Sets.newHashSet(
            HOST,
            PORT,
            PASSWORD,
            ASYNC,
            CACHE_SIZE,
            CACHE_EXPIRE,
            DELIMITER,
            TABLE_NAME,
            PRIMARY_KEY
        );
    }
}
