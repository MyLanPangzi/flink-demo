package com.hiscat.flink.cdc.util;

import com.hiscat.flink.cdc.model.DbProperties;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.net.URI;
import java.sql.*;
import java.util.List;


public class MysqlDdlPrinter {
    private final String db;
    private final JsonNode config;

    public MysqlDdlPrinter(String db, String confPath) throws IOException {
        this.db = db;
        config = ConfigUtil.readConfig(confPath);
    }

    public static void main(String[] args) throws IOException, SQLException {
        new MysqlDdlPrinter(args[0], args[1]).generate();
    }

    private void generate() throws IOException, SQLException {

        try (Connection connection = getConnection(ConfigUtil.getDbProperties(this.config, this.db));
             Statement statement = connection.createStatement();
             Statement pst = connection.createStatement();
             FSDataOutputStream out = getFsDataOutputStream()) {
            statement.execute("use " + db);
            List<String> tables = JsonUtil.getList(this.config.at(String.format("/db/%s", this.db)), "tables");
            for (String table : tables) {
                try (ResultSet rs = pst.executeQuery("show create table " + table)) {
                    if (rs.next()) {
                        String ddl = rs.getString(2);
                        String result = getResult(ddl);
                        out.write(result.getBytes());
                    }
                }
            }
        }
    }

    private FSDataOutputStream getFsDataOutputStream() throws IOException {
        URI uri = URI.create(config.at(String.format("/db/%s/path", this.db)).asText());
        FileSystem fs = FileSystem.get(uri);
        return fs.create(new Path(uri), FileSystem.WriteMode.OVERWRITE);
    }

    private Connection getConnection(DbProperties dbProperties) throws SQLException {
        return DriverManager.getConnection(makeJdbcUrl(dbProperties),
                dbProperties.getUsername(), dbProperties.getPassword());
    }

    private String makeJdbcUrl(DbProperties dbProperties) {
        return String.format(
                "jdbc:mysql://%s:%s/information_schema", dbProperties.getHost(), dbProperties.getPort());
    }

    private static String getResult(String ddl) {
        return appendIfNotExists(appendSemicolon(removeAutoIncrement(appendMetadataCols(ddl))));
    }

    private static String appendMetadataCols(String ddl) {
        int pkIndex = ddl.indexOf("PRIMARY KEY");
        return ddl.substring(0, pkIndex)
                .concat("`metadata_schema_name`   varchar(255) COLLATE utf8mb4_bin NOT NULL,\n" +
                        "`metadata_table_name`    varchar(255) COLLATE utf8mb4_bin NOT NULL,\n" +
                        "`metadata_timestamp`     timestamp(3)                     NULL                        DEFAULT NULL,\n" +
                        "`kafka_topic`            varchar(255) COLLATE utf8mb4_bin                             DEFAULT NULL,\n" +
                        "`kafka_partition`        int                                                          DEFAULT NULL,\n" +
                        "`kafka_offset`           bigint                                                       DEFAULT NULL,\n" +
                        "`kafka_timestamp`        timestamp(3)                     NULL                        DEFAULT NULL,\n" +
                        "`kafka_timestamp_type`   varchar(255) COLLATE utf8mb4_bin                             DEFAULT NULL,\n")
                .concat(ddl.substring(pkIndex));
    }

    private static String removeAutoIncrement(String concat) {
        String autoIncrement = "AUTO_INCREMENT";
        int index = concat.indexOf(autoIncrement);
        if (index<0) {
            return concat;
        }
        return concat.substring(0, index)
                .concat(concat.substring(index + autoIncrement.length()));
    }

    private static String appendSemicolon(String result) {
        return pkAppendMetadataTableNameAndSchemaName(result).concat(";");
    }

    private static String pkAppendMetadataTableNameAndSchemaName(String result) {
        int beginIndex = result.indexOf(")", result.indexOf("PRIMARY KEY ("));
        return result.substring(0, beginIndex)
                .concat(",`metadata_table_name`,`metadata_schema_name`")
                .concat(result.substring(beginIndex));
    }

    private static String appendIfNotExists(String result) {
        String createTable = "CREATE TABLE";
        return result.substring(0, createTable.length())
                .concat(" IF NOT EXISTS ")
                .concat(result.substring(createTable.length()));
    }

}
