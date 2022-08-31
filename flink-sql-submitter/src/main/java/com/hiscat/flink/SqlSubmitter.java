package com.hiscat.flink;

import com.hiscat.flink.cli.CliStatementSplitter;
import com.hiscat.flink.cli.SqlRunnerOptions;
import com.hiscat.flink.function.CallContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.command.SetOperation;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

@Slf4j
public class SqlSubmitter {

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException,
          ParseException {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    final Parser parser = ((StreamTableEnvironmentImpl) tEnv).getParser();
    List<ModifyOperation> dml = new ArrayList<>();

    for (String stmt :
        CliStatementSplitter.splitContent(getSql(SqlRunnerOptions.parseFromArgs(args)))) {
      stmt = stmt.trim();
      if (stmt.endsWith(";")) {
        stmt = stmt.substring(0, stmt.length() - 1).trim();
      }

      if (stmt.trim().isEmpty()) {
        continue;
      }
      if (stmt.startsWith("call")) {
        call(
            stmt,
            CallContext.builder().args(ParameterTool.fromArgs(args)).env(env).tEnv(tEnv).build());
        continue;
      }
      final Operation operation = parser.parse(stmt).get(0);
      if (operation instanceof SetOperation) {
        // SET
        tEnv.getConfig()
            .getConfiguration()
            .setString(
                ((SetOperation) operation)
                    .getKey()
                    .orElseThrow(() -> new RuntimeException("set key not exist")),
                ((SetOperation) operation)
                    .getValue()
                    .orElseThrow(() -> new RuntimeException("set value not exist")));
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

  private static String getSql(SqlRunnerOptions options) throws IOException {
    if (options.getSqlFile().startsWith("classpath:")) {
      return IOUtils.toString(options.getSqlFileInputStreamFromClasspath(), StandardCharsets.UTF_8);
    }
    URI uri = URI.create(options.getSqlFile());
    FileSystem fs = FileSystem.get(uri);
    return IOUtils.toString(fs.open(new Path(uri)), StandardCharsets.UTF_8);
  }

  private static void call(final String stmt, final CallContext context)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    //noinspection unchecked
    ((Consumer<CallContext>) Class.forName(stmt.split(" ")[1]).newInstance()).accept(context);
  }
}
