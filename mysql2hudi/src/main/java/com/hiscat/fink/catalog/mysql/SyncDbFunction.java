package com.hiscat.fink.catalog.mysql;

import lombok.SneakyThrows;
import net.qtt.winter.flink.sql.runner.function.CallContext;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;

import java.util.HashMap;
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
        final MysqlCatalog mysql = (MysqlCatalog) tEnv.getCatalog(srcCatalogName).orElseThrow(() -> new RuntimeException(srcCatalogName + " catalog not exists"));

        final String[] destCatalogDb = configuration.getString(DEST_DB).split("\\.");
        final String destCatalogName = destCatalogDb[0];
        final Catalog hudi = tEnv.getCatalog(destCatalogName).orElseThrow(() -> new RuntimeException(destCatalogName + " catalog not exists"));
        final Catalog defaultCatalog = tEnv.getCatalog("default_catalog").orElseThrow(() -> new RuntimeException("default catalog not exists"));

        final String mysqlDb = srcCatalogDb[1];
        // create hudi table 1. if not exists create 2. exists merge :TODO
        // create cdc table
        final StatementSet statementSet = tEnv.createStatementSet();
        mysql.listTables(mysqlDb).forEach(t -> {
            try {
                final ResolvedCatalogTable mysqlTable = ((ResolvedCatalogTable) mysql.getTable(new ObjectPath(mysqlDb, t)));
                final ObjectPath hudiTable = new ObjectPath(destCatalogDb[1], t);
                hudi.createTable(hudiTable, new ResolvedCatalogTable(mysqlTable, mysqlTable.getResolvedSchema()), true);
                final HashMap<String, String> options = makeCdcOptions(mysql, mysqlDb, t);
                final ObjectPath cdcTable = new ObjectPath("default_database", t);
                defaultCatalog.createTable(cdcTable, mysqlTable.copy(options), true);
                statementSet.addInsertSql(String.format("INSERT INTO %s.%s SELECT * FROM %s", destCatalogName, hudiTable, cdcTable));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        statementSet.execute();
        tEnv.executeSql("show tables").print();
//        sql.append("END;");
    }

    private HashMap<String, String> makeCdcOptions(final MysqlCatalog mysql, final String mysqlDb, final String t) {
        final HashMap<String, String> options = new HashMap<>();
        options.put("connector", "mysql-cdc");
        // TODO: obtain hostname port from url
        options.put("hostname", "localhost");
        options.put("port", "3306");
        options.put("username", mysql.getUsername());
        options.put("password", mysql.getPassword());
        options.put("database-name", mysqlDb);
        options.put("table-name", t);
        return options;
    }
}
