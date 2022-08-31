package com.hiscat.flink.cdc.util;

import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import lombok.SneakyThrows;
import org.apache.flink.configuration.ConfigOption;

public class MysqlSourceOptionsPrinter {
  public static void main(String[] args) {
    //
    Arrays.stream(MySqlSourceOptions.class.getDeclaredFields())
        .filter(f -> Modifier.isStatic(f.getModifiers()))
        .map(MysqlSourceOptionsPrinter::apply)
        .forEach(System.out::println);
  }

  @SneakyThrows
  private static Object apply(Field f) {
    ConfigOption<?> option = (ConfigOption<?>) f.get(null);
    return String.format("SET %s=%s;", option.key(), option.defaultValue());
  }
}
