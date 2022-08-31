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

  private String sqlFile;

  public static SqlRunnerOptions parseFromArgs(String[] args) throws ParseException {
    final CommandLine cli =
        new DefaultParser().parse(new Options().addOption(OPTION_FILE), args, true);

    return SqlRunnerOptions.builder().sqlFile(cli.getOptionValue(OPTION_FILE.getOpt())).build();
  }

  public InputStream getSqlFileInputStreamFromClasspath() {
    return this.getClass()
        .getResourceAsStream(String.format("%s", sqlFile.replace("classpath:", "")));
  }
}
