package org.apache.flink.table.catalog.hive.client;

import com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.util.StringUtils;

public class HiveShimV239Amzn0 extends HiveShimV236 {

  @lombok.SneakyThrows
  @Override
  public IMetaStoreClient getHiveMetastoreClient(HiveConf conf) {
    HiveMetaHookLoader hookLoader = tbl -> {
      try {
        if (tbl == null) {
          return null;
        } else {
          HiveStorageHandler storageHandler = HiveUtils.getStorageHandler(conf,
              tbl.getParameters().get("storage_handler"));
          return storageHandler == null ? null : storageHandler.getMetaHook();
        }
      } catch (HiveException e) {
        HiveUtils.LOG.error(StringUtils.stringifyException(e));
        throw new MetaException("Failed to load storage handler:  " + e.getMessage());
      }
    };

    return new AWSCatalogMetastoreClient(conf, hookLoader);
  }
}
