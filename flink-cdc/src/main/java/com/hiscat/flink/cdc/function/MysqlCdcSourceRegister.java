package com.hiscat.flink.cdc.function;

import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.CHUNK_META_GROUP_SIZE;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.CONNECTION_POOL_SIZE;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.CONNECT_MAX_RETRIES;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.CONNECT_TIMEOUT;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.DATABASE_NAME;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.HEARTBEAT_INTERVAL;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.HOSTNAME;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.PASSWORD;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.PORT;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_STARTUP_MODE;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.SERVER_ID;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.SERVER_TIME_ZONE;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.TABLE_NAME;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.USERNAME;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hiscat.flink.TopicGetter;
import com.hiscat.flink.cdc.model.CdcPayload;
import com.hiscat.flink.cdc.model.DbProperties;
import com.hiscat.flink.function.CallContext;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.JdbcUrlUtils;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.DebeziumOptions;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

public class MysqlCdcSourceRegister implements Consumer<CallContext> {

  public static final String MYSQL_CDC_SOURCE = "cdc";
  public static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
  public static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
  private static final ConfigOption<String> ARN =
      ConfigOptions.key("arn").stringType().noDefaultValue();
  private static final ConfigOption<String> REGION =
      ConfigOptions.key("region").stringType().noDefaultValue();
  private static final ConfigOption<Long> CACHE_SIZE =
      ConfigOptions.key("cache-size").longType().defaultValue(1000L);
  private static final ConfigOption<Duration> CACHE_EXPIRED =
      ConfigOptions.key("cache-expired-ms")
          .durationType()
          .defaultValue(Duration.ofMillis(60 * 1000));
  public static final String TABLE_TOPIC_MAPPING_PREFIX = "table-topic-mapping.";

  @SneakyThrows
  @Override
  public void accept(CallContext context) {
    StreamTableEnvironment tEnv = context.getTEnv();
    registerGetTopicFunction(tEnv);
    Configuration options = tEnv.getConfig().getConfiguration();
    SingleOutputStreamOperator<CdcPayload> ds =
        context
            .getEnv()
            .fromSource(
                makeMysqlCdcSource(options), WatermarkStrategy.noWatermarks(), MYSQL_CDC_SOURCE)
            .uid(MYSQL_CDC_SOURCE)
            .name(MYSQL_CDC_SOURCE);
    tEnv.createTemporaryView(MYSQL_CDC_SOURCE, tEnv.fromDataStream(ds));
  }

  private void registerGetTopicFunction(StreamTableEnvironment tEnv) {
    Configuration options = tEnv.getConfig().getConfiguration();
    Map<String, String> map = options.toMap();
    Map<String, String> tableTopicMapping =
        map.keySet().stream()
            .filter(key -> key.startsWith(TABLE_TOPIC_MAPPING_PREFIX))
            .collect(toMap(k -> k.substring(TABLE_TOPIC_MAPPING_PREFIX.length()), map::get));
    tEnv.createTemporaryFunction(
        "get_topic",
        new TopicGetter(tableTopicMapping, options.get(CACHE_SIZE), options.get(CACHE_EXPIRED)));
  }

  private MySqlSource<CdcPayload> makeMysqlCdcSource(Configuration options) throws IOException {
    MySqlSourceBuilder<CdcPayload> builder = MySqlSource.builder();
    DbProperties secret = getDbProperties(options);
    return builder
        // required options
        .hostname(secret.getHost())
        .port(secret.getPort())
        .username(secret.getUsername())
        .password(secret.getPassword())
        .startupOptions(getStartupOptions(options.get(SCAN_STARTUP_MODE)))
        .databaseList(options.get(DATABASE_NAME))
        .tableList(getTableNames(options))
        .serverId(options.get(SERVER_ID))
        .scanNewlyAddedTableEnabled(options.get(SCAN_NEWLY_ADDED_TABLE_ENABLED))
        // optional options
        .serverTimeZone(options.get(SERVER_TIME_ZONE))
        .heartbeatInterval(options.get(HEARTBEAT_INTERVAL))
        .connectMaxRetries(options.get(CONNECT_MAX_RETRIES))
        .connectTimeout(options.get(CONNECT_TIMEOUT))
        .connectionPoolSize(options.get(CONNECTION_POOL_SIZE))
        // optimization options
        .splitSize(options.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE))
        .fetchSize(options.get(SCAN_SNAPSHOT_FETCH_SIZE))
        .distributionFactorUpper(options.get(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND))
        .distributionFactorLower(options.get(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND))
        .splitMetaGroupSize(options.get(CHUNK_META_GROUP_SIZE))
        // other options
        .jdbcProperties(JdbcUrlUtils.getJdbcProperties(options.toMap()))
        .debeziumProperties(DebeziumOptions.getDebeziumProperties(options.toMap()))

        // deserializer
        .deserializer(new CdcPayloadDebeziumDeserializationSchema())
        .build();
  }

  private String getTableNames(Configuration options) {
    Set<String> tables =
        options.toMap().keySet().stream()
            .filter(key -> key.startsWith(TABLE_TOPIC_MAPPING_PREFIX))
            .map(key -> key.substring(TABLE_TOPIC_MAPPING_PREFIX.length()))
            .collect(toSet());
    tables.addAll(Arrays.asList(options.getString(TABLE_NAME, "").split(",")));
    return String.join(",", tables);
  }

  private DbProperties getDbProperties(Configuration options) throws IOException {
    String username = options.get(USERNAME);
      return DbProperties.builder()
          .username(username)
          .password(options.get(PASSWORD))
          .host(options.get(HOSTNAME))
          .port(options.get(PORT))
          .build();
  }

  private static StartupOptions getStartupOptions(String modeString) {

    switch (modeString.toLowerCase()) {
      case SCAN_STARTUP_MODE_VALUE_INITIAL:
        return StartupOptions.initial();

      case SCAN_STARTUP_MODE_VALUE_LATEST:
        return StartupOptions.latest();

      default:
        throw new ValidationException(
            String.format(
                "Invalid value for option '%s'. Supported values are [%s, %s], but was: %s",
                SCAN_STARTUP_MODE.key(),
                SCAN_STARTUP_MODE_VALUE_INITIAL,
                SCAN_STARTUP_MODE_VALUE_LATEST,
                modeString));
    }
  }

  private static class CdcPayloadDebeziumDeserializationSchema
      implements DebeziumDeserializationSchema<CdcPayload> {

    private transient ObjectMapper objectMapper;
    private transient JsonConverter valueConverter;
    private transient JsonConverter keyConverter;
    private transient boolean initialized;

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<CdcPayload> out) throws Exception {
      if (!initialized) {
        initialize();
      }
      byte[] key =
          keyConverter.fromConnectData(
              sourceRecord.topic(), sourceRecord.keySchema(), sourceRecord.key());
      byte[] value =
          valueConverter.fromConnectData(
              sourceRecord.topic(), sourceRecord.valueSchema(), sourceRecord.value());
      ObjectNode keyNode = (ObjectNode) this.objectMapper.readTree(key);
      JsonNode valueNode = objectMapper.readTree(value);
      JsonNode db = valueNode.at("/source/db");
      keyNode.set("db", db);
      JsonNode table = valueNode.at("/source/table");
      keyNode.set("table", table);
      out.collect(
          CdcPayload.builder()
              .key(keyNode.toString())
              .value(valueNode.toString())
              .db(db.asText())
              .table(table.asText())
              .ts(valueNode.at("/source/ts_ms").asLong())
              .build());
    }

    @Override
    public TypeInformation<CdcPayload> getProducedType() {
      return TypeInformation.of(CdcPayload.class);
    }

    private void initialize() {
      initialized = true;

      objectMapper = new ObjectMapper();

      valueConverter = new JsonConverter();
      Map<String, Object> valueConfigs = new HashMap<>(3);
      valueConfigs.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
      valueConfigs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
      valueConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
      valueConverter.configure(valueConfigs);

      keyConverter = new JsonConverter();
      Map<String, Object> keyConfigs = new HashMap<>(3);
      keyConfigs.put(ConverterConfig.TYPE_CONFIG, ConverterType.KEY.getName());
      keyConfigs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
      keyConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
      keyConverter.configure(keyConfigs);
    }
  }
}
