package com.hiscat.flink.cdc.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class JsonUtil {

  static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

  static Map<String, String> getMap(JsonNode tree, String field) {
    JsonNode node = tree.get(field);
    if (node.isNull()) {
      return Collections.emptyMap();
    }
    ArrayNode single = (ArrayNode) node;
    Map<String, String> singleMap = new HashMap<>(single.size());
    for (JsonNode jsonNode : single) {
      String property = jsonNode.fieldNames().next();
      singleMap.put(property, jsonNode.get(property).asText());
    }
    return singleMap;
  }

  static List<String> getList(JsonNode tree, String field) {
    JsonNode node = tree.get(field);
    if (node.isNull()) {
      return Collections.emptyList();
    }
    ArrayNode single = (ArrayNode) node;
    List<String> result = new ArrayList<>(single.size());
    for (JsonNode jsonNode : single) {
      result.add(jsonNode.asText());
    }
    return result;
  }
}
