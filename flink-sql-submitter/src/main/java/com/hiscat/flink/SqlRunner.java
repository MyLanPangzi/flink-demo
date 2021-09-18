package com.hiscat.flink;

import com.hiscat.flink.cli.CliStatementSplitter;
import com.hiscat.flink.cli.SqlRunnerOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
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

@Slf4j
public class SqlRunner {

    public static void main(String[] args) throws ParseException, IOException {
        final SqlRunnerOptions options = SqlRunnerOptions.parseFromArgs(args);
        final String sql;
        if (options.getEnableExternalSql()) {
            sql = new String(Files.readAllBytes(Paths.get(options.getSqlFile())));
        } else {
            sql = IOUtils.toString(options.getSqlFileInputStreamFromClasspath(), StandardCharsets.UTF_8);
        }
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        final StreamTableEnvironmentImpl impl = (StreamTableEnvironmentImpl) tEnv;
        final Parser parser = impl.getParser();

        List<ModifyOperation> dml = new ArrayList<>();

        for (String stmt : CliStatementSplitter.splitContent(sql)) {
            stmt = stmt.trim();
            if (stmt.endsWith(";")) {
                stmt = stmt.substring(0, stmt.length() - 1).trim();
            }

            if (stmt.trim().isEmpty()) {
                continue;
            }
            final Operation operation = parser.parse(stmt).get(0);
            if (operation instanceof SetOperation) {
                // SET
                callSet(tEnv, (SetOperation) operation);
            } else if (operation instanceof CatalogSinkModifyOperation) {
                // INSERT INTO/OVERWRITE
                dml.add((ModifyOperation) operation);
            } else {
                // fallback to default implementation
                executeOperation(tEnv, operation);
            }
        }
        if (!dml.isEmpty()) {
            impl.executeInternal(dml);
        }

    }

    private static void callSet(final StreamTableEnvironment tEnv, final SetOperation operation) {
        tEnv.getConfig().getConfiguration().setString(operation.getKey().get(), operation.getValue().get());
    }

    private static void executeOperation(final StreamTableEnvironment tEnv, final Operation operation) {
        ((StreamTableEnvironmentImpl) tEnv).executeInternal(operation);
    }
}
