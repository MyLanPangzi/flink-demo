package com.hiscat.flink.cdc.util;

import java.io.IOException;

public class HiveDdlGenerator extends DdlGenerator {

  public HiveDdlGenerator(String target, String db, String confPath) throws IOException {
    super(target, db, confPath);
  }

  public static void main(String[] args) throws IOException {
    new HiveDdlGenerator(args[0], args[1], args[2]).generate();
  }

  protected String formatColsSql(String table) {
    return String.format(
        "select group_concat(case DATA_TYPE\n" +
                "                        when 'bit' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'int', 'COMMENT', CONCAT('\\'',COLUMN_COMMENT,'\\''))\n" +
                "                        when 'tinyint' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'int', 'COMMENT', CONCAT('\\'',COLUMN_COMMENT,'\\''))\n" +
                "                        when 'smallint' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'int', 'COMMENT', CONCAT('\\'',COLUMN_COMMENT,'\\''))\n" +
                "                        when 'int' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'int', 'COMMENT', CONCAT('\\'',COLUMN_COMMENT,'\\''))\n" +
                "                        when 'bigint' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'bigint', 'COMMENT', CONCAT('\\'',COLUMN_COMMENT,'\\''))\n" +
                "                        when 'float' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'float', 'COMMENT', CONCAT('\\'',COLUMN_COMMENT,'\\''))\n" +
                "                        when 'double' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'double', 'COMMENT', CONCAT('\\'',COLUMN_COMMENT,'\\''))\n" +
                "                        when 'decimal' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), COLUMN_TYPE, 'COMMENT', CONCAT('\\'',COLUMN_COMMENT,'\\''))\n" +
                "                        when 'char' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string', 'COMMENT', CONCAT('\\'',COLUMN_COMMENT,'\\''))\n" +
                "                        when 'varchar' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string', 'COMMENT', CONCAT('\\'',COLUMN_COMMENT,'\\''))\n" +
                "                        when 'enum' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string', 'COMMENT', CONCAT('\\'',COLUMN_COMMENT,'\\''))\n" +
                "                        when 'longtext' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string', 'COMMENT', CONCAT('\\'',COLUMN_COMMENT,'\\''))\n" +
                "                        when 'text' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string', 'COMMENT', CONCAT('\\'',COLUMN_COMMENT,'\\''))\n" +
                "                        when 'mediumtext' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string', 'COMMENT', CONCAT('\\'',COLUMN_COMMENT,'\\''))\n" +
                "                        when 'json' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string', 'COMMENT', CONCAT('\\'',COLUMN_COMMENT,'\\''))\n" +
                "                        when 'set' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string', 'COMMENT', CONCAT('\\'',COLUMN_COMMENT,'\\''))\n" +
                "                        when 'binary' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'binary', 'COMMENT', CONCAT('\\'',COLUMN_COMMENT,'\\''))\n" +
                "                        when 'varbinary' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'binary', 'COMMENT', CONCAT('\\'',COLUMN_COMMENT,'\\''))\n" +
                "                        when 'blob' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'binary', 'COMMENT', CONCAT('\\'',COLUMN_COMMENT,'\\''))\n" +
                "                        when 'longblob' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'binary', 'COMMENT', CONCAT('\\'',COLUMN_COMMENT,'\\''))\n" +
                "                        when 'mediumblob' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'binary', 'COMMENT', CONCAT('\\'',COLUMN_COMMENT,'\\''))\n" +
                "                        when 'time' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string', 'COMMENT', CONCAT('\\'',COLUMN_COMMENT,'\\''))\n" +
                "                        when 'date' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string', 'COMMENT', CONCAT('\\'',COLUMN_COMMENT,'\\''))\n" +
                "                        when 'timestamp' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string', 'COMMENT', CONCAT('\\'',COLUMN_COMMENT,'\\''))\n" +
                "                        when 'datetime' THEN CONCAT_WS(' ', CONCAT('`', COLUMN_NAME, '`'), 'string', 'COMMENT', CONCAT('\\'',COLUMN_COMMENT,'\\''))\n" +
                "                        ELSE 'other'\n" +
                "                        END separator ',\\n') as cols,\n" +
                "       GROUP_CONCAT(CONCAT('IF(op <> \\'d\\', after.`', COLUMN_NAME, '`, before.`', COLUMN_NAME, '`)')\n" +
                "                    SEPARATOR ',\\n')         as insert_cols\n" +
                "from information_schema.COLUMNS\n" +
                "where TABLE_SCHEMA = '%s' and TABLE_NAME = '%s'\n" +
                "order by ORDINAL_POSITION",
        this.db,
        table);
  }
}
