package com.hiscat.fink.catalog.mysql;

import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SyncDbParams implements Serializable {
    Schema schema;
    OutputTag<Row> tag;
    String db;
    String table;
    ObjectPath path;
}
