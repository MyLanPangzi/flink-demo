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

package org.apache.flink.connector.jdbc.catalog;

import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectTypeMapper;
import org.apache.flink.connector.jdbc.dialect.mysql.MySqlTypeMapper;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.*;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.IDENTIFIER;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/** Catalog for MySQL. */
@Internal
public class MySqlCatalog extends AbstractJdbcCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlCatalog.class);

    private final JdbcDialectTypeMapper dialectTypeMapper;

    private static final Set<String> builtinDatabases =
            new HashSet<String>() {
                {
                    add("information_schema");
                    add("mysql");
                    add("performance_schema");
                    add("sys");
                }
            };

    public MySqlCatalog(
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl) {
        super(catalogName, defaultDatabase, username, pwd, baseUrl);

        String driverVersion =
                Preconditions.checkNotNull(getDriverVersion(), "Driver version must not be null.");
        String databaseVersion =
                Preconditions.checkNotNull(
                        getDatabaseVersion(), "Database version must not be null.");
        LOG.info("Driver version: {}, database version: {}", driverVersion, databaseVersion);
        this.dialectTypeMapper = new MySqlTypeMapper(databaseVersion, driverVersion);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return extractColumnValuesBySQL(
                defaultUrl,
                "SELECT `SCHEMA_NAME` FROM `INFORMATION_SCHEMA`.`SCHEMATA`;",
                1,
                dbName -> !builtinDatabases.contains(dbName));
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
        throws DatabaseNotExistException, CatalogException {

        Preconditions.checkState(
            !org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly(databaseName),
            "Database name must not be blank.");
        if (listDatabases().contains(databaseName)) {
            return new CatalogDatabaseImpl(Collections.emptyMap(), null);
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
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
            Optional<UniqueConstraint> primaryKey =
                getPrimaryKey(metaData, getSchemaName(tablePath), getTableName(tablePath));

            PreparedStatement ps =
                conn.prepareStatement(
                    String.format("SELECT * FROM %s;", getSchemaTableName(tablePath)));

            ResultSetMetaData resultSetMetaData = ps.getMetaData();

            String[] columnNames = new String[resultSetMetaData.getColumnCount()];
            DataType[] types = new DataType[resultSetMetaData.getColumnCount()];

            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                columnNames[i - 1] = resultSetMetaData.getColumnName(i);
                types[i - 1] = fromJDBCType(tablePath, resultSetMetaData, i);
                if (resultSetMetaData.isNullable(i) == ResultSetMetaData.columnNoNulls) {
                    types[i - 1] = types[i - 1].notNull();
                }
//                if (isPk(primaryKey, columnNames[i - 1])) {
//                    types[i - 1] = types[i - 1].notNull();
//                }
            }

            Schema.Builder schemaBuilder = Schema.newBuilder().fromFields(columnNames, types);
            primaryKey.ifPresent(
                pk -> schemaBuilder.primaryKeyNamed(pk.getName(), pk.getColumns()));
            Schema tableSchema = schemaBuilder.build();

            Map<String, String> props = new HashMap<>();
            props.put(CONNECTOR.key(), IDENTIFIER);
            props.put(URL.key(), dbUrl);
            props.put(USERNAME.key(), username);
            props.put(PASSWORD.key(), pwd);
            props.put(TABLE_NAME.key(), getSchemaTableName(tablePath));
            return CatalogTable.of(tableSchema, null, Lists.newArrayList(), props);
        } catch (Exception e) {
            throw new CatalogException(
                String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    private boolean isPk(Optional<UniqueConstraint> primaryKey, String columnName) {
        return primaryKey.get().getColumns().contains(columnName);
    }


    // ------ tables ------

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        Preconditions.checkState(
                StringUtils.isNotBlank(databaseName), "Database name must not be blank.");
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        return extractColumnValuesBySQL(
                baseUrl + databaseName,
                "SELECT TABLE_NAME FROM information_schema.`TABLES` WHERE TABLE_SCHEMA = ?",
                1,
                null,
                databaseName);
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        return !extractColumnValuesBySQL(
                        baseUrl,
                        "SELECT TABLE_NAME FROM information_schema.`TABLES` "
                                + "WHERE TABLE_SCHEMA=? and TABLE_NAME=?",
                        1,
                        null,
                        tablePath.getDatabaseName(),
                        tablePath.getObjectName())
                .isEmpty();
    }

    protected List<String> extractColumnValuesBySQL(
        String connUrl,
        String sql,
        int columnIndex,
        Predicate<String> filterFunc,
        Object... params) {

        List<String> columnValues = Lists.newArrayList();

        try (Connection conn = DriverManager.getConnection(connUrl, username, pwd);
            PreparedStatement ps = conn.prepareStatement(sql)) {
            if (Objects.nonNull(params) && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
            }
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String columnValue = rs.getString(columnIndex);
                if (Objects.isNull(filterFunc) || filterFunc.test(columnValue)) {
                    columnValues.add(columnValue);
                }
            }
            return columnValues;
        } catch (Exception e) {
            throw new CatalogException(
                String.format(
                    "The following SQL query could not be executed (%s): %s", connUrl, sql),
                e);
        }
    }

    private String getDatabaseVersion() {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            return conn.getMetaData().getDatabaseProductVersion();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed in getting MySQL version by %s.", defaultUrl), e);
        }
    }

    private String getDriverVersion() {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            String driverVersion = conn.getMetaData().getDriverVersion();
            Pattern regexp = Pattern.compile("\\d+?\\.\\d+?\\.\\d+");
            Matcher matcher = regexp.matcher(driverVersion);
            return matcher.find() ? matcher.group(0) : null;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed in getting MySQL driver version by %s.", defaultUrl), e);
        }
    }

    /** Converts MySQL type to Flink {@link DataType}. */
    protected DataType fromJDBCType(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        return dialectTypeMapper.mapping(tablePath, metadata, colIndex);
    }

    protected String getTableName(ObjectPath tablePath) {
        return tablePath.getObjectName();
    }

    protected String getSchemaName(ObjectPath tablePath) {
        return null;
    }

    protected String getSchemaTableName(ObjectPath tablePath) {
        return tablePath.getObjectName();
    }
}
