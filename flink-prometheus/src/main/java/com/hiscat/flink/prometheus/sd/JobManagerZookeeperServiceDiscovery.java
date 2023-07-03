package com.hiscat.flink.prometheus.sd;



import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.CreateMode;

import java.util.Properties;

import static com.hiscat.flink.prometheus.sd.ZookeeperServiceDiscoveryOptions.JM_PATH;
import static com.hiscat.flink.prometheus.sd.ZookeeperServiceDiscoveryOptions.JOB_NAME;


public class JobManagerZookeeperServiceDiscovery extends BaseZookeeperServiceDiscovery{
    @Override
    protected String makePath(Properties properties) {
        return properties.getProperty(JM_PATH.key()) + "/" + System.getProperty(JOB_NAME.key());
    }

    @Override
    protected CreateMode getCreateMode() {
        return CreateMode.EPHEMERAL;
    }

}
