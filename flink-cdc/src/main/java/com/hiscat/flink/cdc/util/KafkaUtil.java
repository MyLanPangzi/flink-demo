package com.hiscat.flink.cdc.util;

import static java.util.stream.Collectors.toList;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;

public class KafkaUtil {
  public static void main(String[] args) {
    Map<String, Object> props = new HashMap<>();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, args[0]);
    try (Admin admin = Admin.create(props)) {
      List<String> topics =
          admin.listTopics().listings().get().stream().map(TopicListing::name).collect(toList());
      Map<String, TopicDescription> map = admin.describeTopics(topics).all().get();
      List<String> result =
          map.keySet().stream()
              .sorted(Comparator.comparingInt(o -> map.get(o).partitions().size()))
              .map(k -> String.format("topic:%s,partitions:%s", k, map.get(k).partitions().size()))
              .collect(toList());
      result.forEach(System.out::println);
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
    }
  }
}
