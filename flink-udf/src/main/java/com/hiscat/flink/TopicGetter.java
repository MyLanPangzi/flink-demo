package com.hiscat.flink;

import static java.util.stream.Collectors.toList;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

public class TopicGetter extends ScalarFunction {
  private final List<Pattern> patterns;
  private transient Cache<String, String> cache;
  private final Map<String, String> tableTopicMapping;
  private final long cacheSize;
  private final Duration cacheExpiredMs;

  @Override
  public void open(FunctionContext context) {
    this.cache =
        CacheBuilder.newBuilder()
            .maximumSize(this.cacheSize)
            .expireAfterAccess(this.cacheExpiredMs)
            .build();
  }

  public TopicGetter(
      Map<String, String> tableTopicMapping, long cacheSize, Duration cacheExpiredMs) {
    this.patterns = tableTopicMapping.keySet().stream().map(Pattern::compile).collect(toList());
    this.tableTopicMapping = tableTopicMapping;
    this.cacheSize = cacheSize;
    this.cacheExpiredMs = cacheExpiredMs;
  }

  public String eval(String db, String table) {
    String identifier = String.format("%s.%s", db, table);
    String topic = cache.getIfPresent(identifier);
    if (topic != null) {
      if (topic.isEmpty()) {
        return null;
      }
      return topic;
    }
    topic =
        patterns.stream()
            .filter(p -> p.matcher(identifier).matches())
            .findFirst()
            .map(p -> tableTopicMapping.get(p.pattern()))
            .orElse("");
    cache.put(identifier, topic);
    if (topic.isEmpty()) {
      return null;
    }
    return topic;
  }
}
