package com.hiscat.flink.custrom.connector.json.ogg;

import com.hiscat.flink.custrom.connector.json.JsonOptions;
import com.hiscat.flink.custrom.connector.json.JsonRowDataSerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.Objects;

import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 * Serialization schema that serializes an object of Flink Table/SQL internal data structure {@link
 * RowData} into a Canal JSON bytes.
 *
 * @see <a href="https://github.com/alibaba/canal">Alibaba Canal</a>
 */
public class OggJsonSerializationSchema implements SerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private static final StringData OP_INSERT = StringData.fromString("I");
    private static final StringData OP_DELETE = StringData.fromString("D");

    private transient GenericRowData reuse;

    /**
     * The serializer to serialize Canal JSON data.
     */
    private final JsonRowDataSerializationSchema jsonSerializer;

    public OggJsonSerializationSchema(
            RowType rowType,
            TimestampFormat timestampFormat,
            JsonFormatOptions.MapNullKeyMode mapNullKeyMode,
            String mapNullKeyLiteral,
            boolean encodeDecimalAsPlainNumber) {
        jsonSerializer =
                new JsonRowDataSerializationSchema(
                        createJsonRowType(fromLogicalToDataType(rowType)),
                        timestampFormat,
                        mapNullKeyMode,
                        mapNullKeyLiteral,
                        encodeDecimalAsPlainNumber);
    }

    @Override
    public void open(InitializationContext context) {
        reuse = new GenericRowData(2);
    }

    @Override
    public byte[] serialize(RowData row) {
        try {
            StringData opType = rowKind2String(row.getRowKind());
            ArrayData arrayData = new GenericArrayData(new RowData[]{row});
            reuse.setField(0, arrayData);
            reuse.setField(1, opType);
            return jsonSerializer.serialize(reuse);
        } catch (Throwable t) {
            throw new RuntimeException("Could not serialize row '" + row + "'.", t);
        }
    }

    private StringData rowKind2String(RowKind rowKind) {
        switch (rowKind) {
            case INSERT:
            case UPDATE_AFTER:
                return OP_INSERT;
            case UPDATE_BEFORE:
            case DELETE:
                return OP_DELETE;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported operation '" + rowKind + "' for row kind.");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OggJsonSerializationSchema that = (OggJsonSerializationSchema) o;
        return Objects.equals(jsonSerializer, that.jsonSerializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jsonSerializer);
    }

    private static RowType createJsonRowType(DataType databaseSchema) {
        // Canal JSON contains other information, e.g. "database", "ts"
        // but we don't need them
        // and we don't need "old" , because can not support UPDATE_BEFORE,UPDATE_AFTER
        return (RowType)
                DataTypes.ROW(
                        DataTypes.FIELD("before", databaseSchema),
                        DataTypes.FIELD("after", databaseSchema),
                        DataTypes.FIELD("op_type", DataTypes.STRING()))
                        .getLogicalType();
    }
}
