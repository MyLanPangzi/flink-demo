package com.hiscat;

import org.apache.flink.table.client.SqlClient;

/**
 * MacOS
 * 1. /etc/profile or ~/.bashrc or ~/.zshrc add export FLINK_CONF_DIR=
 *    add flink-conf.yaml execution.target: local
 * 2. restart idea
 * 3. just like normal sql-client.sh test
 * https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/table/sqlclient/#execute-a-set-of-sql-statements
 */
public class SqlClientTest {
  public static void main(String[] args) {
    SqlClient.main(args);
  }
}
