package com.hiscat.flink.prometheus.sd;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class ZookeeperServiceDiscoveryOptions {
    public static final ConfigOption<String> TM_PATH = ConfigOptions.key("tm.path")
            .stringType()
            .noDefaultValue()
            .withDescription("zk tm path");
    public static final ConfigOption<String> JM_PATH = ConfigOptions.key("jm.path")
            .stringType()
            .noDefaultValue()
            .withDescription("zk jm path");
    public static final ConfigOption<String> SD_IDENTIFIER = ConfigOptions.key("sd.identifier")
            .stringType()
            .defaultValue("none")
            .withDescription("service discovery identifier ");

    public static final ConfigOption<String> ZK_QUORUM = ConfigOptions.key("zk.quorum")
            .stringType()
            .noDefaultValue()
            .withDescription("service discovery zk quorum ");

//  ==================  cli option ==========================
    public static final ConfigOption<String> CONTAINER_ID = ConfigOptions.key("container_id")
            .stringType()
            .noDefaultValue()
            .withDescription("yarn container_id");

    public static final ConfigOption<String> ROLE = ConfigOptions.key("role")
            .stringType()
            .noDefaultValue()
            .withDescription("env variable role");
    public static final ConfigOption<String> JOB_NAME = ConfigOptions.key("jobName")
            .stringType()
            .noDefaultValue()
            .withDescription("job name ");
}
