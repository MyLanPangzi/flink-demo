package com.hiscat.flink.custrom.connector.nsq;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NsqOptions implements Serializable {

    public static final ConfigOption<String> HOST = ConfigOptions.key("host").stringType().noDefaultValue();
    public static final ConfigOption<String> TOPIC = ConfigOptions.key("topic").stringType().noDefaultValue();
    public static final ConfigOption<String> FORMAT = ConfigOptions.key("format").stringType().noDefaultValue();
    public static final ConfigOption<String> CHANNEL = ConfigOptions.key("channel").stringType().noDefaultValue();

    private String host;
    private String topic;
    private String channel;

}
