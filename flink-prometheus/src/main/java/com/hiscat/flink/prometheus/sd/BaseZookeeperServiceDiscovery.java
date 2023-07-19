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
        // 增加个1min重试，如果路径存在，拿一下zknode的内容和本地的地址和端口比对一下，如果一致，就不用注册临时节点，如果不一致，删掉重新注册，实现推空保护
        registerNodeWithRetry(address, properties);
    }

    private void initClient(Properties properties) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 100);
        client = CuratorFrameworkFactory.newClient(properties.getProperty(ZK_QUORUM.key()), retryPolicy);
        client.start();
    }
    private void registerNodeWithRetry(InetSocketAddress address, Properties properties) throws InterruptedException {
        int retryInterval = 60 * 1000; // 1 minute
        int maxRetries = 10; // to prevent infinite retries

        for (int i = 0; i < maxRetries; i++) {
            try {
                String path = makePath(properties);
                Stat stat = client.checkExists().forPath(path);
                if (stat != null) {
                    byte[] data = client.getData().forPath(path);
                    String nodeContent = new String(data, StandardCharsets.UTF_8);
                    String expectedContent = new String(makeServerSetData(address), StandardCharsets.UTF_8);
                    if (nodeContent.equals(expectedContent)) {
                        return;
                    } else {
                        client.delete().forPath(path);
                    }
                }

                client
                        .create()
                        .creatingParentsIfNeeded()
                        .withMode(getCreateMode())
                        .forPath(path, makeServerSetData(address));
                return;
            } catch (KeeperException.NodeExistsException e) {
                // ignore and retry
            } catch (Exception e) {
                if(i == maxRetries - 1) {
                    throw new RuntimeException("Failed to register node after " + maxRetries + " attempts: " + e.getMessage(), e);
                }
                // Log warning
            }

            Thread.sleep(retryInterval);
        }

        throw new InterruptedException("Node registration interrupted");
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
