package com.hiscat.flink.prometheus.sd;

import static com.hiscat.flink.prometheus.sd.ZookeeperServiceDiscoveryOptions.ROLE;

public class ZookeeperServiceDiscoveryFactory implements ServiceDiscoveryFactory {
    @Override
    public String identifier() {
        return "zookeeper";
    }

    @Override
    public ServiceDiscovery create() {
        if (currentNodeIsJobManager()) {
            return new JobManagerZookeeperServiceDiscovery();
        }
        return new TaskManagerZookeeperServiceDiscovery();
    }

    private static boolean currentNodeIsJobManager() {
        return "jm".equals(System.getProperty(ROLE.key()));
    }

}
