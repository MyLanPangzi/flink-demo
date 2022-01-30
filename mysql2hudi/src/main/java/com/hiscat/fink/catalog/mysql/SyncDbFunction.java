package com.hiscat.fink.catalog.mysql;

import com.hiscat.flink.function.CallContext;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.MySqlDeserializationConverterFactory;
import com.ververica.cdc.connectors.mysql.table.MySqlReadableMetadata;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.MetadataConverter;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.*;
import java.util.function.Consumer;

import static com.ververica.cdc.connectors.mysql.table.MySqlReadableMetadata.DATABASE_NAME;
import static java.util.stream.Collectors.toList;

/**
 * TODO: per table config. per table sql. multiple table merge into one
 */
@SuppressWarnings("unused")
public class SyncDbFunction implements Consumer<CallContext> {
    private static final ConfigOption<String> SRC_DB = ConfigOptions.key("custom.sync-db.source.db").stringType().noDefaultValue();
    private static final ConfigOption<String> DEST_DB = ConfigOptions.key("custom.sync-db.dest.db").stringType().noDefaultValue();

    private static String getKey(RowData r) {
        final JoinedRowData rowData = (JoinedRowData) r;
        return rowData.getString(rowData.getArity() - 3).toString() + "." + rowData.getString(rowData.getArity() - 2).toString();
    }

    /**
     * 1. create hudi table
     * 2. make db.table -> deserializerSchema map
     * 3. make db.table -> convert map
     * 4. start single cdc source
     * 1. deserializer SourceRecord to RowData
     * 5 key by db.table
     * 1. RowData to Row
     * 2. side out
     * 6 write hudi
     *
     * @param context 参数
     */
    @SneakyThrows
    @Override
    public void accept(final CallContext context) {
        final StreamTableEnvironment tEnv = context.getTEnv();
        final Configuration configuration = tEnv.getConfig().getConfiguration();

        final String[] srcCatalogDb = configuration.getString(SRC_DB).split("\\.");
        final MysqlCdcCatalog mysql = (MysqlCdcCatalog) tEnv.getCatalog(srcCatalogDb[0]).orElseThrow(() -> new RuntimeException(srcCatalogDb[0] + " catalog not exists"));
        final String mysqlDb = srcCatalogDb[1];

        final String[] destCatalogDb = configuration.getString(DEST_DB).split("\\.");
        final String hudiCatalogName = destCatalogDb[0];
        final Catalog hudi = tEnv.getCatalog(hudiCatalogName).orElseThrow(() -> new RuntimeException(hudiCatalogName + " catalog not exists"));
        final String hudiDb = destCatalogDb[1];

        Map<String, RowDataDebeziumDeserializeSchema> debeziumDeserializeSchemaMap = new HashMap<>();
        Map<String, RowRowConverter> converterMap = new HashMap<>();
        List<SyncDbParams> params = new ArrayList<>();

//         动态分流写入
        mysql.listTables(mysqlDb).forEach(t -> {
            try {

                final ObjectPath mysqlTablePath = new ObjectPath(mysqlDb, t);
                final ResolvedCatalogTable mysqlTable = ((ResolvedCatalogTable) mysql.getTable(mysqlTablePath));
                final String fullName = mysqlTablePath.getFullName();
                hudi.createTable(new ObjectPath(hudiDb, t), new ResolvedCatalogTable(mysqlTable.copy(Collections.emptyMap()), mysqlTable.getResolvedSchema()), true);
                debeziumDeserializeSchemaMap.put(
                    fullName,
                    RowDataDebeziumDeserializeSchema.newBuilder()
                        .setPhysicalRowType((RowType) mysqlTable.getResolvedSchema().toPhysicalRowDataType().getLogicalType())
                        .setUserDefinedConverterFactory(MySqlDeserializationConverterFactory.instance())
                        .setMetadataConverters(new MetadataConverter[]{DATABASE_NAME.getConverter(),
                            MySqlReadableMetadata.TABLE_NAME.getConverter(),
                            MySqlReadableMetadata.OP_TS.getConverter()})
                        .setResultTypeInfo(TypeInformation.of(RowData.class))
                        .build()
                );
                converterMap.put(fullName, RowRowConverter.create(mysqlTable.getResolvedSchema().toPhysicalRowDataType()));
                final OutputTag<Row> tag = new OutputTag<Row>(fullName) {
                };
                final List<DataTypes.Field> fields = mysqlTable.getResolvedSchema().getColumns()
                    .stream().map(c -> DataTypes.FIELD(c.getName(), c.getDataType())).collect(toList());
                final Schema schema = Schema.newBuilder()
                    .column("f0", DataTypes.ROW(fields.toArray(new DataTypes.Field[]{}))).build();
                final SyncDbParams p = SyncDbParams.builder()
                    .table(t)
                    .path(new ObjectPath(mysqlDb, t))
                    .tag(tag)
                    .schema(schema)

                    .build();
                params.add(p);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        final MySqlSource<RowData> source = getMySqlSource(srcCatalogDb, mysql, debeziumDeserializeSchemaMap);

        final SingleOutputStreamOperator<Void> process = context.getEnv()
            .fromSource(
                source, WatermarkStrategy.noWatermarks(), "mysql"
            )
            .keyBy(SyncDbFunction::getKey)
            .process(new StringRowDataVoidKeyedProcessFunction(converterMap));

        final StatementSet set = tEnv.createStatementSet();
        params.forEach(p -> {
            tEnv.createTemporaryView(p.table, process.getSideOutput(p.tag), p.schema);
            set.addInsertSql(String.format("INSERT INTO %s.%s SELECT f0.* FROM %s", hudiCatalogName, p.path.getFullName(), p.table));
//            tEnv.sqlQuery("select f0.* FROM " + p.table).executeInsert("hudi." + p.path.getFullName());
        });
        set.execute();

//        context.getEnv().execute();
    }

    private MySqlSource<RowData> getMySqlSource(final String[] srcCatalogDb, final MysqlCdcCatalog mysql, final Map<String, RowDataDebeziumDeserializeSchema> maps) {

        // TODO: extract hostname port from url
        return MySqlSource.<RowData>builder()
            .hostname("localhost")
            .port(3306)
            .username(mysql.getUsername())
            .password(mysql.getPassword())
            .databaseList(srcCatalogDb)
            .tableList(".*")
            .deserializer(new CompositeDebeziuDeserializationSchema(maps))
            .build();
    }

    private static class CompositeDebeziuDeserializationSchema implements DebeziumDeserializationSchema<RowData> {

        private final Map<String, RowDataDebeziumDeserializeSchema> deserializationSchemaMap;

        public CompositeDebeziuDeserializationSchema(final Map<String, RowDataDebeziumDeserializeSchema> deserializationSchemaMap) {
            this.deserializationSchemaMap = deserializationSchemaMap;
        }

        @Override
        public void deserialize(final SourceRecord record, final Collector<RowData> out) throws Exception {
            final Struct value = (Struct) record.value();
            final Struct source = value.getStruct("source");
            final String db = source.getString("db");
            final String table = source.getString("table");
            deserializationSchemaMap.get(db + "." + table).deserialize(record, out);
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return TypeInformation.of(RowData.class);
        }
    }

    private static class StringRowDataVoidKeyedProcessFunction extends KeyedProcessFunction<String, RowData, Void> {

        //        private transient Map<String, OutputTag<Row>> tags;
        private Map<String, RowRowConverter> converters;

        public StringRowDataVoidKeyedProcessFunction(final Map<String, RowRowConverter> rowRowConverter) {
            this.converters = rowRowConverter;
        }

        @Override
        public void processElement(final RowData value, final KeyedProcessFunction<String, RowData, Void>.Context ctx, final Collector<Void> out) throws Exception {
            ctx.output(new OutputTag<Row>(ctx.getCurrentKey()) {
            }, this.converters.get(ctx.getCurrentKey()).toExternal(value));
        }
    }
}
