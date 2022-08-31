/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hiscat.flink.connector.jdbc.table;

import static java.util.stream.Collectors.toList;
import static org.apache.flink.util.Preconditions.checkState;

import com.hiscat.flink.connector.jdbc.table.converter.PartialUpdateMySQLRowConverter;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSink;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSource;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

/**
 * Factory for creating configured instances of {@link JdbcDynamicTableSource} and {@link
 * JdbcDynamicTableSink}.
 */
@Internal
public class PartialUpdateJdbcDynamicTableFactory implements DynamicTableSinkFactory {

  public static final String IDENTIFIER = "partial-jdbc";
  public static final ConfigOption<String> URL =
      ConfigOptions.key("url")
          .stringType()
          .noDefaultValue()
          .withDescription("The JDBC database URL.");
  public static final ConfigOption<String> TABLE_NAME =
      ConfigOptions.key("table-name")
          .stringType()
          .noDefaultValue()
          .withDescription("The JDBC table name.");
  public static final ConfigOption<String> USERNAME =
      ConfigOptions.key("username")
          .stringType()
          .noDefaultValue()
          .withDescription("The JDBC user name.");
  public static final ConfigOption<String> PASSWORD =
      ConfigOptions.key("password")
          .stringType()
          .noDefaultValue()
          .withDescription("The JDBC password.");
  private static final ConfigOption<String> DRIVER =
      ConfigOptions.key("driver")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "The class name of the JDBC driver to use to connect to this URL. "
                  + "If not set, it will automatically be derived from the URL.");
  public static final ConfigOption<Duration> MAX_RETRY_TIMEOUT =
      ConfigOptions.key("connection.max-retry-timeout")
          .durationType()
          .defaultValue(Duration.ofSeconds(60))
          .withDescription("Maximum timeout between retries.");

  // write config options
  private static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS =
      ConfigOptions.key("sink.buffer-flush.max-rows")
          .intType()
          .defaultValue(100)
          .withDescription(
              "The flush max size (includes all append, upsert and delete records), over this number"
                  + " of records, will flush data.");
  private static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL =
      ConfigOptions.key("sink.buffer-flush.interval")
          .durationType()
          .defaultValue(Duration.ofSeconds(1))
          .withDescription(
              "The flush interval mills, over this time, asynchronous threads will flush data.");
  private static final ConfigOption<Integer> SINK_MAX_RETRIES =
      ConfigOptions.key("sink.max-retries")
          .intType()
          .defaultValue(3)
          .withDescription("The max retry times if writing records to database failed.");
  public static final String PREPARE_STATEMENT_PREFIX = "prepare-statement";

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    final FactoryUtil.TableFactoryHelper helper =
        FactoryUtil.createTableFactoryHelper(this, context);
    final ReadableConfig config = helper.getOptions();

    validateConfigOptions(config);
    JdbcOptions jdbcOptions = getJdbcOptions(config);
    ResolvedSchema tableSchema = context.getCatalogTable().getResolvedSchema();
    Map<String, String> map = ((Configuration) config).toMap();

    return new PartialUpdateJdbcDynamicTableSink(
        getPartialUpdateOptions(tableSchema, map),
        jdbcOptions,
        getJdbcExecutionOptions(config),
        getJdbcDmlOptions(jdbcOptions, tableSchema),
        tableSchema);
  }

  private List<PartialUpdateOptions> getPartialUpdateOptions(
      ResolvedSchema tableSchema, Map<String, String> map) {
    RowType rowType = (RowType) tableSchema.toPhysicalRowDataType().getLogicalType();
    return map.keySet().stream()
        .filter(k -> k.startsWith(PREPARE_STATEMENT_PREFIX))
        .map(
            k -> {
              String[] split = k.split("\\.");
              List<String> fields = Arrays.asList(map.get(k).split(","));
              List<Integer> indexes =
                  fields.stream()
                      .map(f -> tableSchema.getColumnNames().indexOf(f))
                      .collect(toList());
              return PartialUpdateOptions.builder()
                  .tag(split[1])
                  .fields(fields)
                  .converter(new PartialUpdateMySQLRowConverter(rowType, indexes))
                  .build();
            })
        .collect(toList());
  }

  private JdbcOptions getJdbcOptions(ReadableConfig readableConfig) {
    final String url = readableConfig.get(URL);
    final JdbcOptions.Builder builder =
        JdbcOptions.builder()
            .setDBUrl(url)
            .setTableName(readableConfig.get(TABLE_NAME))
            .setDialect(JdbcDialects.get(url).get())
            .setParallelism(readableConfig.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null))
            .setConnectionCheckTimeoutSeconds(
                (int) readableConfig.get(MAX_RETRY_TIMEOUT).getSeconds());

    readableConfig.getOptional(DRIVER).ifPresent(builder::setDriverName);
    readableConfig.getOptional(USERNAME).ifPresent(builder::setUsername);
    readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
    return builder.build();
  }

  private JdbcExecutionOptions getJdbcExecutionOptions(ReadableConfig config) {
    final JdbcExecutionOptions.Builder builder = new JdbcExecutionOptions.Builder();
    builder.withBatchSize(config.get(SINK_BUFFER_FLUSH_MAX_ROWS));
    builder.withBatchIntervalMs(config.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis());
    builder.withMaxRetries(config.get(SINK_MAX_RETRIES));
    return builder.build();
  }

  private JdbcDmlOptions getJdbcDmlOptions(JdbcOptions jdbcOptions, ResolvedSchema schema) {
    String[] keyFields =
        schema.getPrimaryKey().map(pk -> pk.getColumns().toArray(new String[0])).orElse(null);

    return JdbcDmlOptions.builder()
        .withTableName(jdbcOptions.getTableName())
        .withDialect(jdbcOptions.getDialect())
        .withFieldNames(schema.getColumnNames().toArray(new String[0]))
        .withKeyFields(keyFields)
        .build();
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    Set<ConfigOption<?>> requiredOptions = new HashSet<>();
    requiredOptions.add(URL);
    requiredOptions.add(TABLE_NAME);
    return requiredOptions;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    Set<ConfigOption<?>> optionalOptions = new HashSet<>();
    optionalOptions.add(DRIVER);
    optionalOptions.add(USERNAME);
    optionalOptions.add(PASSWORD);
    optionalOptions.add(SINK_BUFFER_FLUSH_MAX_ROWS);
    optionalOptions.add(SINK_BUFFER_FLUSH_INTERVAL);
    optionalOptions.add(SINK_MAX_RETRIES);
    optionalOptions.add(FactoryUtil.SINK_PARALLELISM);
    optionalOptions.add(MAX_RETRY_TIMEOUT);
    return optionalOptions;
  }

  private void validateConfigOptions(ReadableConfig config) {
    String jdbcUrl = config.get(URL);
    final Optional<JdbcDialect> dialect = JdbcDialects.get(jdbcUrl);
    checkState(dialect.isPresent(), "Cannot handle such jdbc url: " + jdbcUrl);

    checkAllOrNone(config, new ConfigOption[] {USERNAME, PASSWORD});

    if (config.get(SINK_MAX_RETRIES) < 0) {
      throw new IllegalArgumentException(
          String.format(
              "The value of '%s' option shouldn't be negative, but is %s.",
              SINK_MAX_RETRIES.key(), config.get(SINK_MAX_RETRIES)));
    }

    if (config.get(MAX_RETRY_TIMEOUT).getSeconds() <= 0) {
      throw new IllegalArgumentException(
          String.format(
              "The value of '%s' option must be in second granularity and shouldn't be smaller than 1 second, but is %s.",
              MAX_RETRY_TIMEOUT.key(),
              config.get(
                  ConfigOptions.key(MAX_RETRY_TIMEOUT.key()).stringType().noDefaultValue())));
    }
  }

  private void checkAllOrNone(ReadableConfig config, ConfigOption<?>[] configOptions) {
    int presentCount = 0;
    for (ConfigOption<?> configOption : configOptions) {
      if (config.getOptional(configOption).isPresent()) {
        presentCount++;
      }
    }
    String[] propertyNames =
        Arrays.stream(configOptions).map(ConfigOption::key).toArray(String[]::new);
    Preconditions.checkArgument(
        configOptions.length == presentCount || presentCount == 0,
        "Either all or none of the following options should be provided:\n"
            + String.join("\n", propertyNames));
  }
}
