package com.hiscat.flink.custrom.connector.nsq;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class NsqDynamicTableSource implements ScanTableSource {

    private final NsqOptions nsqOptions;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private final DataType consumedDataTypes;

    public NsqDynamicTableSource(final NsqOptions nsqOptions,
                                 final DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
                                 final DataType consumedDataTypes) {
        this.nsqOptions = nsqOptions;
        this.consumedDataTypes = consumedDataTypes;
        this.decodingFormat = decodingFormat;
    }


    @Override
    public DynamicTableSource copy() {
        return new NsqDynamicTableSource(nsqOptions, decodingFormat, consumedDataTypes);
    }

    @Override
    public String asSummaryString() {
        return "nsq dynamic table source";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return decodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(final ScanContext runtimeProviderContext) {
        final DeserializationSchema<RowData> deserializationSchema = decodingFormat.createRuntimeDecoder(runtimeProviderContext, consumedDataTypes);
        return SourceFunctionProvider.of(new NsqSourceFunction(nsqOptions, deserializationSchema), false);
    }
}
