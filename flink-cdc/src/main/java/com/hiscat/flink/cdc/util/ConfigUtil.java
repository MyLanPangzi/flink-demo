package com.hiscat.flink.cdc.util;

import com.hiscat.flink.cdc.model.DbProperties;
import org.apache.commons.io.IOUtils;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;

public class ConfigUtil {
    public static JsonNode readConfig(String path) throws IOException {
        URI uri = URI.create(path);
        final JsonNode config;
        try (FSDataInputStream fsDataInputStream = FileSystem.get(uri).open(new Path(uri))) {
            String yaml = IOUtils.toString(fsDataInputStream, StandardCharsets.UTF_8);
            config = JsonUtil.OBJECT_MAPPER.readTree(yaml);
        }
        return config;
    }

    static DbProperties getDbProperties(JsonNode config, String db) throws IOException {
        JsonNode node = config.at(String.format("/db/%s", db));
        JsonNode usernameNode = node.get("username");
        if (usernameNode != null && !usernameNode.isNull()) {
            String password = node.get("password").asText();
            String host = node.get("host").asText();
            int port = node.get("port").asInt();
            return new DbProperties(usernameNode.asText(), password, host, port);
        }
        String arn = config.at(String.format("/db/%s/arn", db)).asText();
        return SecretUtil.getSecret(arn, "ap-southeast-1");
    }
}
