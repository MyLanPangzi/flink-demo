package com.hiscat.flink.prometheus.sd;

import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.CreateMode;

import java.util.Properties;

import static com.hiscat.flink.prometheus.sd.ZookeeperServiceDiscoveryOptions.*;

public class TaskManagerZookeeperServiceDiscovery extends BaseZookeeperServiceDiscovery {


    @Override
    protected String makePath(Properties properties) {
        return properties.getProperty(TM_PATH.key()) + "/"
                + System.getProperty(JOB_NAME.key())
                + "_"
                + System.getProperty(CONTAINER_ID.key());
    }

    @Override
    protected CreateMode getCreateMode() {
        return CreateMode.EPHEMERAL_SEQUENTIAL;
    }

}
