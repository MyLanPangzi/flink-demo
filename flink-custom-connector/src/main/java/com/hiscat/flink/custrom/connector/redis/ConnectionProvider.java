package com.hiscat.flink.custrom.connector.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;

public class ConnectionProvider {
    public static StatefulRedisConnection<String, String> getConnection(RedisOptions options) {
        RedisURI redisUri = RedisURI.builder()
            .withHost(options.getHost())
            .withPort(options.getPort())
            .build();
//        if (options.getPassword() != null) {
//            redisUri.setPassword(DESUtils.decryptFromBase64(options.getPassword()));
//        }
        RedisClient redisClient = RedisClient.create(redisUri);
        return redisClient.connect();
    }
}
