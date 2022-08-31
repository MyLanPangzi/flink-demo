package com.hiscat.flink.connector.jdbc.table;

import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.DRIVER;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_MAX_RETRIES;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.MAX_RETRY_TIMEOUT;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.PASSWORD;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.TABLE_NAME;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.URL;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.USERNAME;
import static org.apache.flink.util.Preconditions.checkState;

import java.util.Arrays;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.Preconditions;

public class RedshiftDynamicTableFactory implements DynamicTableSinkFactory {

  public static final String IDENTIFIER = "redshift";

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    final FactoryUtil.TableFactoryHelper helper =
        FactoryUtil.createTableFactoryHelper(this, context);
    final ReadableConfig config = helper.getOptions();

    helper.validate();
    validateConfigOptions(config);
    JdbcOptions jdbcOptions = getJdbcOptions(config);
    ResolvedSchema tableSchema = context.getCatalogTable().getResolvedSchema();
    return new RedshiftDynamicTableSink(
        jdbcOptions,
        getJdbcExecutionOptions(config),
        getJdbcDmlOptions(jdbcOptions, tableSchema),
        tableSchema);
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

  private JdbcOptions getJdbcOptions(ReadableConfig readableConfig) {
    final String url = readableConfig.get(URL);
    final JdbcOptions.Builder builder =
        JdbcOptions.builder()
            .setDBUrl(url)
            .setTableName(readableConfig.get(TABLE_NAME))
            .setDialect(JdbcDialects.get(url).orElseThrow(()->new NoSuchElementException(url + " no such dialect")))
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
}
