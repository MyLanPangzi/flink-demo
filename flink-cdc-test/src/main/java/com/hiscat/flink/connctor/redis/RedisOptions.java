package com.hiscat.flink.connctor.redis;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
public class RedisOptions implements Serializable {

    public static final ConfigOption<String> HORT =
        ConfigOptions.key("host")
            .stringType()
            .noDefaultValue()
            .withDescription("The JDBC database URL.");

    public static final ConfigOption<String> SINK_KEY_PREFIX =
        ConfigOptions.key("sink.key-prefix")
            .stringType()
            .noDefaultValue()
            .withDescription("The JDBC database URL.");

    public static final ConfigOption<Integer> PORT =
        ConfigOptions.key("port")
            .intType()
            .defaultValue(6379);

    private String host;
    private int port;
    private String keyPrefix;
}
