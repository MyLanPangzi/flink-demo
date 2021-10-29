package com.hiscat.flink.custrom.connector.nsq;

import com.hiscat.flink.custrom.connector.nsq.util.IndexGenerator;
import com.sproutsocial.nsq.Publisher;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.UserCodeClassLoader;

public class NsqSinkFunction extends RichSinkFunction<RowData> {

    private final NsqOptions nsqOptions;
    private final SerializationSchema<RowData> encoder;
    private final IndexGenerator indexGenerator;
    private Publisher publisher;

    public NsqSinkFunction(final NsqOptions nsqOptions,
                           final SerializationSchema<RowData> encoder,
                           final IndexGenerator indexGenerator) {
        this.nsqOptions = nsqOptions;
        this.encoder = encoder;
        this.indexGenerator = indexGenerator;
    }

    @Override
    public void open(final Configuration parameters) throws Exception {
        encoder.open(new SerializationSchema.InitializationContext() {
            @Override
            public MetricGroup getMetricGroup() {
                return getRuntimeContext().getMetricGroup();
            }

            @Override
            public UserCodeClassLoader getUserCodeClassLoader() {
                return (UserCodeClassLoader) getRuntimeContext().getUserCodeClassLoader();
            }
        });
        this.publisher = new Publisher(nsqOptions.getHost());
    }

    @Override
    public void close() {
        publisher.stop();
    }

    @Override
    public void invoke(final RowData value, final Context context) {
        switch (value.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                publisher.publishBuffered(indexGenerator.generate(value), encoder.serialize(value));
                break;
            default:
                break;
        }
    }
}
