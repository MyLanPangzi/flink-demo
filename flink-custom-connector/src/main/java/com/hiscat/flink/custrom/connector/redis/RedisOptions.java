package com.hiscat.flink.custrom.connector.redis;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class RedisOptions implements Serializable {

    public static final ConfigOption<String> HOST = ConfigOptions.key("host").stringType().defaultValue("localhost");
    public static final ConfigOption<Integer> PORT = ConfigOptions.key("port").intType().defaultValue(6379);
    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password").stringType().defaultValue(null);
    public static final ConfigOption<String> DELIMITER = ConfigOptions.key("delimiter").stringType().defaultValue(":");
    public static final ConfigOption<Boolean> ASYNC = ConfigOptions.key("async").booleanType().defaultValue(false);
    public static final ConfigOption<Long> CACHE_SIZE = ConfigOptions.key("cache-size").longType().defaultValue(1000L);
    public static final ConfigOption<Long> CACHE_EXPIRE = ConfigOptions.key("cache-expire-ms").longType().defaultValue(10*1000L);
    public static final ConfigOption<String> TABLE_NAME = ConfigOptions.key("table-name").stringType().noDefaultValue();
    public static final ConfigOption<String> PRIMARY_KEY = ConfigOptions.key("primary-key").stringType().noDefaultValue();
    public static final ConfigOption<Integer> MAX_RETRY_TIMES = ConfigOptions.key("max-retry-times").intType().defaultValue(3);

    private int maxRetryTimes;
    private String host;
    private Integer port;
    private String password;
    private String delimiter;
    private Boolean async;
    private Long cacheSize;
    private Long cacheExpireMs;
    private String tableName;
    private String primaryKey;

}

