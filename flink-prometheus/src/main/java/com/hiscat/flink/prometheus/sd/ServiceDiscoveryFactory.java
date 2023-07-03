package com.hiscat.flink.prometheus.sd;

public interface ServiceDiscoveryFactory {
    String identifier();

    ServiceDiscovery create();
}
