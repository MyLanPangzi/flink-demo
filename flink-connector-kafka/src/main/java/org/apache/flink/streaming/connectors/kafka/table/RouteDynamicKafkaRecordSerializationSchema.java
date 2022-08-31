package org.apache.flink.streaming.connectors.kafka.table;

import java.util.Objects;
import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.table.RouteKafkaDynamicSink.TopicMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.kafka.clients.producer.ProducerRecord;

public class RouteDynamicKafkaRecordSerializationSchema
    implements KafkaRecordSerializationSchema<RowData> {

  private final KafkaRecordSerializationSchema<RowData> rowDataKafkaRecordSerializationSchema;

  private final int topicPos;

  public RouteDynamicKafkaRecordSerializationSchema(
      KafkaRecordSerializationSchema<RowData> rowDataKafkaRecordSerializationSchema,
      int topicMetadataPosition) {
    this.rowDataKafkaRecordSerializationSchema = rowDataKafkaRecordSerializationSchema;
    this.topicPos = topicMetadataPosition;
  }

  @Override
  public void open(InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
    this.rowDataKafkaRecordSerializationSchema.open(context, sinkContext);
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(
      RowData element, KafkaSinkContext context, Long timestamp) {
    ProducerRecord<byte[], byte[]> record =
        this.rowDataKafkaRecordSerializationSchema.serialize(element, context, timestamp);
    Object topic = TopicMetadata.TOPIC.converter.read(element, topicPos);
    if (Objects.isNull(topic)) {
      return record;
    }
    return new ProducerRecord<>(
        topic.toString(),
        record.partition(),
        record.timestamp(),
        record.key(),
        record.value(),
        record.headers());
  }
}
