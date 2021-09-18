package com.hiscat.flink.cli;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.cli.*;

import java.io.InputStream;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SqlRunnerOptions {

    public static final Option OPTION_EXTERNAL_SQL =
        Option.builder("e")
            .required(false)
            .longOpt("enable.external.sql")
            .numberOfArgs(1)
            .argName("read external sql ")
            .desc("Read SQL from external path don't from classpath.")
            .build();

    public static final Option OPTION_INIT_FILE =
        Option.builder("i")
            .required(false)
            .longOpt("init")
            .numberOfArgs(1)
            .argName("initialization file")
            .desc(
                "Script file that used to init the session context. "
                    + "If get error in execution, the sql client will exit. Notice it's not allowed to add query or insert into the init file.")
            .build();

    public static final Option OPTION_FILE =
        Option.builder("f")
            .required(false)
            .longOpt("file")
            .numberOfArgs(1)
            .argName("script file")
            .desc(
                "Script file that should be executed. In this mode, "
                    + "the client will not open an interactive terminal.")
            .build();

    public static final Option OPTION_MODE =
        Option.builder("m")
            .required(false)
            .longOpt("mode")
            .numberOfArgs(1)
            .argName("execute mode")
            .desc("execution mode test or prod")
            .build();

    public static final String TEST = "test";

    private String mode;
    private String initFile;
    private String sqlFile;
    private Boolean enableExternalSql;

    public static SqlRunnerOptions parseFromArgs(String[] args) throws ParseException {
        final CommandLine cli = new DefaultParser()
            .parse(
                new Options()
                    .addOption(OPTION_MODE)
                    .addOption(OPTION_FILE)
                    .addOption(OPTION_EXTERNAL_SQL)
                    .addOption(OPTION_EXTERNAL_SQL),
                args,
                false
            );

        return SqlRunnerOptions.builder()
            .sqlFile(cli.getOptionValue(OPTION_FILE.getOpt()))
            .initFile(cli.getOptionValue(OPTION_EXTERNAL_SQL.getOpt()))
            .mode(cli.getOptionValue(OPTION_MODE.getOpt(), TEST))
            .enableExternalSql(Boolean.valueOf(cli.getOptionValue(OPTION_EXTERNAL_SQL.getOpt(), "false")))
            .build();
    }


    public InputStream getSqlFileInputStreamFromClasspath() {
        return this.getClass().getResourceAsStream(String.format("/%s/%s", mode, sqlFile));
    }

}
