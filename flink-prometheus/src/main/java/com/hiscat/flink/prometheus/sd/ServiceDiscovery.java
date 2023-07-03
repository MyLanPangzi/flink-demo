package com.hiscat.flink.prometheus.sd;

import java.net.InetSocketAddress;
import java.util.Properties;

public interface ServiceDiscovery {
    void register(InetSocketAddress address, Properties properties);

    void close();
}
