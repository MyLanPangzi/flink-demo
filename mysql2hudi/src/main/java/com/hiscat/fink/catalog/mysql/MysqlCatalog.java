/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hiscat.fink.catalog.mysql;

import com.mysql.cj.MysqlType;
import com.mysql.cj.jdbc.DatabaseMetaDataUsingInfoSchema;
import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.*;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/**
 * Catalog for Mysql.
 */
@Internal
public class MysqlCatalog extends AbstractJdbcCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(MysqlCatalog.class);

    public static final String DEFAULT_DATABASE = "mysql";

    // ------ Postgres default objects that shouldn't be exposed to users ------

    private static final Set<String> builtinDatabases =
        new HashSet<String>() {
            {
                add("information_schema");
                add("performance_schema");
                add("sys");
                add("mysql");
            }
        };


    protected MysqlCatalog(
        String catalogName,
        String defaultDatabase,
        String username,
        String pwd,
        String baseUrl) {
        super(catalogName, defaultDatabase, username, pwd, baseUrl);
    }

    public static void main(String[] args) throws DatabaseNotExistException, TableNotExistException {
        final MysqlCatalog catalog = new MysqlCatalog("mysql", "test", "root", "!QAZ2wsx", "jdbc:mysql://localhost:3306/");
        catalog.listDatabases().forEach(System.out::println);
        catalog.listTables("test").forEach(System.out::println);
        System.out.println(catalog.getTable(new ObjectPath("test", "test")).getOptions());
    }
    // ------ databases ------

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> mysqlDatabases = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {

            PreparedStatement ps = conn.prepareStatement("show databases");

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                String dbName = rs.getString(1);
                if (!builtinDatabases.contains(dbName)) {
                    mysqlDatabases.add(rs.getString(1));
                }
            }

            return mysqlDatabases;
        } catch (Exception e) {
            throw new CatalogException(
                String.format("Failed listing database in catalog %s", getName()), e);
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
        throws DatabaseNotExistException, CatalogException {
        if (listDatabases().contains(databaseName)) {
            return new CatalogDatabaseImpl(Collections.emptyMap(), null);
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    // ------ tables ------

    @Override
    public List<String> listTables(String databaseName)
        throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        // get all schemas
        try (Connection conn = DriverManager.getConnection(baseUrl + databaseName, username, pwd)) {
            PreparedStatement ps =
                conn.prepareStatement("show tables");

            ResultSet rs = ps.executeQuery();

            List<String> tables = new ArrayList<>();

            while (rs.next()) {
                tables.add(rs.getString(1));
            }

            return tables;
        } catch (Exception e) {
            throw new CatalogException(
                String.format("Failed listing database in catalog %s", getName()), e);
        }
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
        throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        String dbUrl = baseUrl + tablePath.getDatabaseName();
        try (Connection conn = DriverManager.getConnection(dbUrl, username, pwd)) {
            DatabaseMetaData metaData = conn.getMetaData();
            final DatabaseMetaDataUsingInfoSchema infoSchema = (DatabaseMetaDataUsingInfoSchema) metaData;
            final ResultSet rs = infoSchema.getColumns(tablePath.getDatabaseName(), tablePath.getDatabaseName(), tablePath.getObjectName(), null);

            final Schema.Builder builder = Schema.newBuilder();
            List<Column> cols = new ArrayList<>();
            // this method have some bug. :TODO
            while (rs.next()) {
                final String columnName = rs.getString("COLUMN_NAME");
                final String typeName = rs.getString("TYPE_NAME");
                final AbstractDataType<?> dataType = getDataType(typeName);
                builder.column(columnName, dataType);
                cols.add(Column.physical(columnName, (DataType) dataType));
            }
            rs.close();

            final ResultSet primaryKeys = infoSchema.getPrimaryKeys(tablePath.getDatabaseName(), tablePath.getDatabaseName(), tablePath.getObjectName());
            while (primaryKeys.next()) {
                builder.primaryKey(primaryKeys.getString("COLUMN_NAME"));
            }
            primaryKeys.close();

            Map<String, String> props = new HashMap<>();
            props.put(CONNECTOR.key(), IDENTIFIER);
            props.put(URL.key(), dbUrl);
            props.put(TABLE_NAME.key(), tablePath.getObjectName());
            props.put(USERNAME.key(), username);
            props.put(PASSWORD.key(), pwd);

            final Schema schema = builder.build();
            final Schema.UnresolvedPrimaryKey unresolvedPrimaryKey = schema.getPrimaryKey().orElseThrow(() -> new RuntimeException(tablePath + "no pk "));
            // because hudi catalog need resolved schema
            final ResolvedSchema resolvedSchema = new ResolvedSchema(cols, Collections.emptyList(), UniqueConstraint.primaryKey(unresolvedPrimaryKey.getConstraintName(), unresolvedPrimaryKey.getColumnNames()));
            return new ResolvedCatalogTable(CatalogTable.of(schema, "", Collections.emptyList(), props), resolvedSchema);
//            return r;
//            final Optional<UniqueConstraint> primaryKey = getPrimaryKey(metaData, tablePath.getDatabaseName(), tablePath.getObjectName());
//            Optional<UniqueConstraint> primaryKey =
//                getPrimaryKey(metaData, pgPath.getPgSchemaName(), pgPath.getPgTableName());
//
//            PreparedStatement ps =
//                conn.prepareStatement(String.format("SELECT * FROM %s;", pgPath.getFullPath()));
//
//            ResultSetMetaData rsmd = ps.getMetaData();
//
//            String[] names = new String[rsmd.getColumnCount()];
//            DataType[] types = new DataType[rsmd.getColumnCount()];
//
//            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
//                names[i - 1] = rsmd.getColumnName(i);
//                types[i - 1] = fromJDBCType(rsmd, i);
//                if (rsmd.isNullable(i) == ResultSetMetaData.columnNoNulls) {
//                    types[i - 1] = types[i - 1].notNull();
//                }
//            }
//
//            TableSchema.Builder tableBuilder = new TableSchema.Builder().fields(names, types);
//            primaryKey.ifPresent(
//                pk ->
//                    tableBuilder.primaryKey(
//                        pk.getName(), pk.getColumns().toArray(new String[0])));
//            TableSchema tableSchema = tableBuilder.build();
//

//            return new CatalogTableImpl(tableSchema, props, "");
        } catch (Exception e) {
            throw new CatalogException(
                String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    private AbstractDataType<?> getDataType(final String typeName) {
        final MysqlType mysqlType = MysqlType.getByName(typeName);
        switch (mysqlType) {
            case BIGINT:
                return DataTypes.BIGINT().notNull();
            case INT:
                return DataTypes.INT().notNull();
            case TIMESTAMP:
                return DataTypes.TIMESTAMP(3);
            case VARCHAR:
                return DataTypes.STRING();

            default:
                throw new RuntimeException("unsupport type" + typeName);
        }
    }

    // Postgres jdbc driver maps several alias to real type, we use real type rather than alias:
    // serial2 <=> int2
    // smallserial <=> int2
    // serial4 <=> serial
    // serial8 <=> bigserial
    // smallint <=> int2
    // integer <=> int4
    // int <=> int4
    // bigint <=> int8
    // float <=> float8
    // boolean <=> bool
    // decimal <=> numeric

    /**
     * Converts Postgres type to Flink {@link DataType}.
     */
    private DataType fromJDBCType(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String typeName = metadata.getColumnTypeName(colIndex);
        System.out.println(typeName);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (typeName) {
            case "BIGINT":
                return DataTypes.BIGINT();
            case "VARCHAR":
                return DataTypes.STRING();
            default:
                throw new UnsupportedOperationException(
                    String.format("Doesn't support Postgres type '%s' yet", typeName));
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {

        List<String> tables = null;
        try {
            tables = listTables(tablePath.getDatabaseName());
        } catch (DatabaseNotExistException e) {
            return false;
        }

        return tables.contains(tablePath.getObjectName());
    }
}
