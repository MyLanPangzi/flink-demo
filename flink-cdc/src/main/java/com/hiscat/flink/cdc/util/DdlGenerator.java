package com.hiscat.flink.cdc.util;

import static java.sql.DriverManager.getConnection;

import com.hiscat.flink.cdc.model.DbProperties;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import org.apache.commons.io.IOUtils;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class DdlGenerator {
  private final String target;
  protected final String db;
  private final JsonNode config;

  public DdlGenerator(String target, String db, String confPath) throws IOException {
    this.target = target;
    this.db = db;
    URI uri = URI.create(confPath);
    try (FSDataInputStream fsDataInputStream = FileSystem.get(uri).open(new Path(uri))) {
      String yaml = IOUtils.toString(fsDataInputStream, StandardCharsets.UTF_8);
      config = JsonUtil.OBJECT_MAPPER.readTree(yaml);
    }
  }

  public static void main(String[] args) throws IOException {
    new DdlGenerator(args[0], args[1], args[2]).generate();
  }

  protected void generate() throws IOException {
    String sb = makeDdl();
    write2s3(sb);
  }

  private String makeDdl() throws IOException {
    DbProperties dbProperties = getDbProperties();
    StringBuilder ddlSb = new StringBuilder();
    StringBuilder dmlSb = new StringBuilder();
    try (Connection connection =
            getConnection(
                getUrl(dbProperties), dbProperties.getUsername(), dbProperties.getPassword());
        Statement st = connection.createStatement();
        ResultSet set = st.executeQuery("set group_concat_max_len=1073741824"); ) {

      String ddlTemplate = readTemplate("ddl");
      String dmlTemplate = readTemplate("dml");
      for (String table :
          JsonUtil.getList(this.config.at(String.format("/db/%s", this.db)), "tables")) {
        try (ResultSet rs = st.executeQuery(formatColsSql(table))) {
          if (rs.next()) {
            String ddl = generateDdl(table, rs, ddlTemplate);
            String dml = generateDml(table, rs, dmlTemplate);
            ddlSb.append(ddl);
            dmlSb.append(dml);
          }
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    if (dmlSb.length() > 0) {
      ddlSb.append("BEGIN STATEMENT SET;\n");
      ddlSb.append(dmlSb);
      ddlSb.append("END;");
    }
    return ddlSb.toString();
  }

  protected String generateDml(String table, ResultSet rs, String template) throws SQLException {
    String insertCols = rs.getString("insert_cols");
    return template
        .replace("{db}", this.db)
        .replace("{table}", table)
        .replace("{insert_cols}", insertCols);
  }

  protected String generateDdl(String table, ResultSet rs, String template) throws SQLException {
    String cols = rs.getString("cols");
    return template.replace("{db}", this.db).replace("{table}", table).replace("{cols}", cols);
  }

  private void write2s3(String sql) throws IOException {
    String s3path = config.at(String.format("/db/%s/s3path", this.db)).asText();
    URI uri = URI.create(s3path);
    try (FSDataOutputStream fsDataOutputStream =
        FileSystem.get(uri).create(new Path(s3path), WriteMode.OVERWRITE)) {
      fsDataOutputStream.write(sql.getBytes());
    }
  }

  private String getUrl(DbProperties dbProperties) {
    return String.format(
        "jdbc:mysql://%s:%s/information_schema", dbProperties.getHost(), dbProperties.getPort());
  }

  private String readTemplate(String sqlType) throws IOException {
    String templateName = String.format("/%s.%s.template", target, sqlType);
    URL resource = this.getClass().getResource(templateName);
    InputStream ddlInputStream = Objects.requireNonNull(resource).openStream();
    return IOUtils.toString(ddlInputStream, StandardCharsets.UTF_8);
  }

  private DbProperties getDbProperties() throws IOException {
    return ConfigUtil.getDbProperties(config, db);
  }

  protected String formatColsSql(String table) {
    return String.format(
        "select group_concat(case DATA_TYPE\n"
            + "                        when 'bit' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'int')\n"
            + "                        when 'bigint' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'bigint')\n"
            + "                        when 'varchar' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string')\n"
            + "                        when 'timestamp'\n"
            + "                            THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string')\n"
            + "                        when 'int' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'int')\n"
            + "                        when 'longtext' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string')\n"
            + "                        when 'enum' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string')\n"
            + "                        when 'text' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string')\n"
            + "                        when 'mediumtext'\n"
            + "                            THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string')\n"
            + "                        when 'json' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string')\n"
            + "                        when 'datetime' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string')\n"
            + "                        when 'set' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string')\n"
            + "                        when 'binary' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string')\n"
            + "                        when 'char' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string')\n"
            + "                        when 'varbinary'\n"
            + "                            THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string')\n"
            + "                        when 'tinyint' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'int')\n"
            + "                        when 'blob' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string')\n"
            + "                        when 'double' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'double')\n"
            + "                        when 'longblob' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string')\n"
            + "                        when 'smallint' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'int')\n"
            + "                        when 'mediumblob'\n"
            + "                            THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string')\n"
            + "                        when 'time' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string')\n"
            + "                        when 'float' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'float')\n"
            + "                        when 'date' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string')\n"
            + "                        when 'decimal' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'),\n"
            + "                                                      COLUMN_TYPE)\n"
            + "                        ELSE 'other'\n"
            + "                        END separator ',\\n') as cols,\n"
            + "       GROUP_CONCAT(CONCAT('IF(op <> \\'d\\', after.`',  COLUMN_NAME, '`, before.`', COLUMN_NAME, '`)')\n"
            + "                    SEPARATOR ',\\n')         as insert_cols\n"
            + "from information_schema.COLUMNS\n"
            + "where TABLE_SCHEMA='%s' and TABLE_NAME = '%s' order by ORDINAL_POSITION",
        this.db,
        table);
  }
}
