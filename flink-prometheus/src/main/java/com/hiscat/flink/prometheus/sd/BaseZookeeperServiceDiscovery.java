package com.hiscat.flink.prometheus.sd;



import org.apache.flink.shaded.curator5.org.apache.curator.RetryPolicy;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.flink.shaded.curator5.org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.CreateMode;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.data.Stat;

import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.Properties;

import static com.hiscat.flink.prometheus.sd.ZookeeperServiceDiscoveryOptions.ZK_QUORUM;

public abstract class BaseZookeeperServiceDiscovery implements ServiceDiscovery {

    private CuratorFramework client;

    @Override
    public void register(InetSocketAddress address, Properties properties) {
        initClient(properties);
        registerNode(address, properties);
    }

    private void initClient(Properties properties) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 100);
        client = CuratorFrameworkFactory.newClient(properties.getProperty(ZK_QUORUM.key()), retryPolicy);
        client.start();
    }


    private void registerNode(InetSocketAddress address, Properties properties) {
        try {
            String path = makePath(properties);
            Stat stat = client.checkExists().forPath(path);
            if (stat != null) {
                client.setData().forPath(path, makeServerSetData(address));
                return;
            }

            client
                    .create()
                    .creatingParentsIfNeeded()
                    .withMode(getCreateMode())
                    .forPath(path, makeServerSetData(address));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract String makePath(Properties properties);

    protected abstract CreateMode getCreateMode();

    private byte[] makeServerSetData(InetSocketAddress address) {
        String jsonFormat = "{\"serviceEndpoint\":{\"host\":\"%s\",\"port\":%d},\"additionalEndpoints\":{},\"status\":\"ALIVE\"}\n";
        try {
            return String.format(jsonFormat, getIpAddress(),
                    address.getPort()).getBytes(StandardCharsets.UTF_8);
        } catch (SocketException | UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private String getIpAddress() throws SocketException, UnknownHostException {
        Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
        while (enumeration.hasMoreElements()) {
            NetworkInterface network = enumeration.nextElement();
            if (network.isVirtual() || !network.isUp()) {
                continue;
            }
            Enumeration<InetAddress> addresses = network.getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress address = addresses.nextElement();
                if (address.isLoopbackAddress()) {
                    continue;
                }
                if (address.isSiteLocalAddress()) {
                    return address.getHostAddress();
                }

            }
        }
        return InetAddress.getLocalHost().getHostAddress();
    }

    @Override
    public void close() {
        client.close();
    }
}
