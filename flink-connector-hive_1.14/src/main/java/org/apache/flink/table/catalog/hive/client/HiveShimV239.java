package org.apache.flink.table.catalog.hive.client;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient;
import com.amazonaws.glue.catalog.metastore.AWSCredentialsProviderFactory;
import com.amazonaws.glue.catalog.metastore.DefaultAWSCredentialsProviderFactory;
import com.amazonaws.glue.catalog.metastore.GlueClientFactory;
import com.amazonaws.glue.catalog.util.MetastoreClientUtils;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.google.common.base.Strings;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

public class HiveShimV239 extends HiveShimV237 {

}
