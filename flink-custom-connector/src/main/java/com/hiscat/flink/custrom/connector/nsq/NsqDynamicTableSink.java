package com.hiscat.flink.custrom.connector.nsq;

import com.hiscat.flink.custrom.connector.nsq.util.IndexGenerator;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class NsqDynamicTableSink implements DynamicTableSink {

    private final NsqOptions nsqOptions;
    private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;
    private final DataType producedDataTypes;
    private final IndexGenerator indexGenerator;

    public NsqDynamicTableSink(final NsqOptions nsqOptions,
                               final EncodingFormat<SerializationSchema<RowData>> encodingFormat,
                               final DataType producedDataTypes, final IndexGenerator indexGenerator) {
        this.nsqOptions = nsqOptions;
        this.encodingFormat = encodingFormat;
        this.producedDataTypes = producedDataTypes;
        this.indexGenerator = indexGenerator;
    }

    @Override
    public ChangelogMode getChangelogMode(final ChangelogMode requestedMode) {
        return ChangelogMode.upsert();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(final Context context) {
        final SerializationSchema<RowData> encoder = encodingFormat.createRuntimeEncoder(context, producedDataTypes);
        return SinkFunctionProvider.of(new NsqSinkFunction(nsqOptions, encoder, indexGenerator));
    }

    @Override
    public DynamicTableSink copy() {
        return new NsqDynamicTableSink(nsqOptions, encodingFormat, producedDataTypes, indexGenerator);
    }

    @Override
    public String asSummaryString() {
        return "nsq dynamic table sink";
    }
}
