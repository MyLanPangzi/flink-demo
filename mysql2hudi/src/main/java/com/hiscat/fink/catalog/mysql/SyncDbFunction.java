package com.hiscat.fink.catalog.mysql;

import com.hiscat.flink.function.CallContext;
import lombok.SneakyThrows;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;

import java.util.function.Consumer;

@SuppressWarnings("unused")
public class SyncDbFunction implements Consumer<CallContext> {
    private static final ConfigOption<String> SRC_DB = ConfigOptions.key("custom.sync-db.source.db").stringType().noDefaultValue();
    private static final ConfigOption<String> DEST_DB = ConfigOptions.key("custom.sync-db.dest.db").stringType().noDefaultValue();

    @SneakyThrows
    @Override
    public void accept(final CallContext context) {
        final StreamTableEnvironment tEnv = context.getTEnv();
        final Configuration configuration = tEnv.getConfig().getConfiguration();

        final String[] srcCatalogDb = configuration.getString(SRC_DB).split("\\.");
        final String srcCatalogName = srcCatalogDb[0];
        final MysqlCdcCatalog mysql = (MysqlCdcCatalog) tEnv.getCatalog(srcCatalogName).orElseThrow(() -> new RuntimeException(srcCatalogName + " catalog not exists"));

        final String[] destCatalogDb = configuration.getString(DEST_DB).split("\\.");
        final String destCatalogName = destCatalogDb[0];
        final Catalog hudi = tEnv.getCatalog(destCatalogName).orElseThrow(() -> new RuntimeException(destCatalogName + " catalog not exists"));
        final Catalog defaultCatalog = tEnv.getCatalog("default_catalog").orElseThrow(() -> new RuntimeException("default catalog not exists"));

        final String mysqlDb = srcCatalogDb[1];
        // create hudi table 1. if not exists create 2. exists merge :TODO
        final StatementSet statementSet = tEnv.createStatementSet();
        mysql.listTables(mysqlDb).forEach(t -> {
            try {
                final ObjectPath cdcTablePath = new ObjectPath(mysqlDb, t);
                final ResolvedCatalogTable mysqlTable = ((ResolvedCatalogTable) mysql.getTable(cdcTablePath));
                hudi.createTable(new ObjectPath(destCatalogDb[1], t), new ResolvedCatalogTable(mysqlTable, mysqlTable.getResolvedSchema()), true);
                statementSet.addInsertSql(String.format("INSERT INTO %s.%s SELECT * FROM %s.%s", destCatalogName, new ObjectPath(destCatalogDb[1], t), srcCatalogName, cdcTablePath.getFullName()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        statementSet.execute();
    }

}
