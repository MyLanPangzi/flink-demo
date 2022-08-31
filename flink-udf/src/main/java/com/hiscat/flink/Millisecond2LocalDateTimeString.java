package com.hiscat.flink;

import java.time.Instant;
import java.time.ZoneId;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

@SuppressWarnings("unused")
public class Millisecond2LocalDateTimeString extends ScalarFunction {

  public String eval(@DataTypeHint("BIGINT") Long mill ) {
    return Instant.ofEpochMilli(mill).atZone(ZoneId.systemDefault()).toLocalDateTime().toString();
  }

  public static void main(String[] args) {
    System.out.println(Instant.ofEpochMilli(0).atZone(ZoneId.systemDefault()).toLocalDateTime());
    //
  }
}
