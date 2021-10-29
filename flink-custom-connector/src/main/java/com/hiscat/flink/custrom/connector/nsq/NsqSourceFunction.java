package com.hiscat.flink.custrom.connector.nsq;

import com.sproutsocial.nsq.MessageDataHandler;
import com.sproutsocial.nsq.Subscriber;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.UserCodeClassLoader;

import java.io.IOException;
import java.util.concurrent.Executors;

@Slf4j
public class NsqSourceFunction extends RichSourceFunction<RowData> {

    private final NsqOptions nsqOptions;
    private final DeserializationSchema<RowData> deserializationSchema;
    private Subscriber subscriber;
    private boolean running;

    public NsqSourceFunction(final NsqOptions nsqOptions, final DeserializationSchema<RowData> deserializationSchema) {
        this.nsqOptions = nsqOptions;
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void open(final Configuration parameters) throws Exception {
        running = true;
        subscriber = new Subscriber(nsqOptions.getHost());
        subscriber.getClient().setExecutor(Executors.newSingleThreadExecutor());
        deserializationSchema.open(new DeserializationSchema.InitializationContext() {
            @Override
            public MetricGroup getMetricGroup() {
                return getRuntimeContext().getMetricGroup();
            }

            @Override
            public UserCodeClassLoader getUserCodeClassLoader() {
                return (UserCodeClassLoader) NsqSourceFunction.class.getClassLoader();
            }
        });
    }

    @Override
    public void run(final SourceContext<RowData> ctx) throws InterruptedException {
        subscriber.subscribe(nsqOptions.getTopic(), nsqOptions.getChannel(), (MessageDataHandler) data -> {
            try {
                ctx.collect(deserializationSchema.deserialize(data));
            } catch (IOException e) {
                log.error("subscribe error : {}", e.getMessage(), e);
            }
        });
        while (running) {
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
        subscriber.stop();
    }
}
