package com.hiscat.flink;

import com.hiscat.flink.cli.CliStatementSplitter;
import com.hiscat.flink.cli.SqlRunnerOptions;
import com.hiscat.flink.function.CallContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.command.SetOperation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

@Slf4j
public class SqlRunner {

    public static void main(String[] args) throws ParseException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        final SqlRunnerOptions options = SqlRunnerOptions.parseFromArgs(args);
        if (options.getEnableHiveSupport()) {
            registerHiveCatalog(tEnv);
        }

        final Parser parser = ((StreamTableEnvironmentImpl) tEnv).getParser();
        List<ModifyOperation> dml = new ArrayList<>();

        final String sql;
        if (options.getEnableExternalSql()) {
            sql = new String(Files.readAllBytes(Paths.get(options.getSqlFile())));
        } else {
            sql = IOUtils.toString(options.getSqlFileInputStreamFromClasspath(), StandardCharsets.UTF_8);
        }
        for (String stmt : CliStatementSplitter.splitContent(sql)) {
            stmt = stmt.trim();
            if (stmt.endsWith(";")) {
                stmt = stmt.substring(0, stmt.length() - 1).trim();
            }

            if (stmt.trim().isEmpty()) {
                continue;
            }
            if (stmt.startsWith("call")) {
                call(stmt, CallContext.builder()
                    .args(ParameterTool.fromArgs(args))
                    .env(env)
                    .tEnv(tEnv)
                    .build());
                continue;
            }
            final Operation operation = parser.parse(stmt).get(0);
            if (operation instanceof SetOperation) {
                // SET
                tEnv.getConfig().getConfiguration()
                    .setString(((SetOperation) operation).getKey().orElseThrow(() -> new RuntimeException("set key not exist")),
                        ((SetOperation) operation).getValue().orElseThrow(() -> new RuntimeException("set value not exist")));
            } else if (operation instanceof CatalogSinkModifyOperation) {
                // INSERT INTO/OVERWRITE
                dml.add((ModifyOperation) operation);
            } else {
                // fallback to default implementation
                ((StreamTableEnvironmentImpl) tEnv).executeInternal(operation);
            }
        }
        if (!dml.isEmpty()) {
            ((StreamTableEnvironmentImpl) tEnv).executeInternal(dml);
        }

    }

    private static void call(final String stmt, final CallContext context) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        //noinspection unchecked
        ((Consumer<CallContext>) Class.forName(stmt.split(" ")[1]).newInstance()).accept(context);
    }

    private static void registerHiveCatalog(final StreamTableEnvironment tEnv) {
        HiveCatalog hive = new HiveCatalog("hive_catalog", "flink_sql", "/opt/hive-conf");
        tEnv.registerCatalog("hive_catalog", hive);
    }

}
