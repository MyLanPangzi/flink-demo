package com.hiscat.flink.custrom.connector.nsq;

import com.hiscat.flink.custrom.connector.nsq.util.IndexGenerator;
import com.hiscat.flink.custrom.connector.nsq.util.IndexGeneratorFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.*;
import org.apache.flink.table.types.DataType;

import java.util.Collections;
import java.util.Set;

public class NsqDynamicTableFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "nsq";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Sets.newHashSet(
            NsqOptions.HOST,
            NsqOptions.TOPIC,
            NsqOptions.FORMAT
        );
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.singleton(NsqOptions.CHANNEL);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(final Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        helper.validate();

        final NsqOptions nsqOptions = NsqOptions.builder()
            .host(helper.getOptions().get(NsqOptions.HOST))
            .topic(helper.getOptions().get(NsqOptions.TOPIC))
            .build();

        final EncodingFormat<SerializationSchema<RowData>> encodingFormat =
            helper.discoverEncodingFormat(SerializationFormatFactory.class, NsqOptions.FORMAT);

        final DataType producedDataTypes = context.getCatalogTable().getResolvedSchema().toSinkRowDataType();

        final IndexGenerator indexGenerator = IndexGeneratorFactory
            .createIndexGenerator(nsqOptions.getTopic(), context.getCatalogTable().getSchema());

        return new NsqDynamicTableSink(nsqOptions, encodingFormat, producedDataTypes, indexGenerator);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(final Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        helper.validate();

        final String channel = helper.getOptions().getOptional(NsqOptions.CHANNEL)
            .orElseThrow(() -> new IllegalArgumentException("miss channel arg"));

        final NsqOptions nsqOptions = NsqOptions.builder()
            .host(helper.getOptions().get(NsqOptions.HOST))
            .topic(helper.getOptions().get(NsqOptions.TOPIC))
            .channel(channel)
            .build();

        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
            helper.discoverDecodingFormat(DeserializationFormatFactory.class, NsqOptions.FORMAT);
        final DataType consumedDataTypes = context.getCatalogTable().getResolvedSchema().toSinkRowDataType();

        return new NsqDynamicTableSource(nsqOptions, decodingFormat, consumedDataTypes);
    }
}
