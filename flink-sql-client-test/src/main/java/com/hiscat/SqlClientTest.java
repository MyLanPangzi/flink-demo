package com.hiscat;

import org.apache.flink.table.client.SqlClient;

/**
 * MacOS
 * 1. /etc/profile or ~/.bashrc or ~/.zshrc add export FLINK_CONF_DIR=
 *    add flink-conf.yaml execution.target: local
 * 2. restart idea
 * 3. just like normal sql-client.sh test
 */
public class SqlClientTest {
  public static void main(String[] args) {
    SqlClient.main(args);
  }
}
