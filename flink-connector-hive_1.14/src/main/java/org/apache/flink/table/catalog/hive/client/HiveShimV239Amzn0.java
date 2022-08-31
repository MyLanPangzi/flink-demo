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

public class HiveShimV239Amzn0 extends HiveShimV237 {

    @lombok.SneakyThrows
    @Override
    public IMetaStoreClient getHiveMetastoreClient(HiveConf conf) {
        return new AWSCatalogMetastoreClient.Builder()
                .withHiveConf(conf)
                .withCatalogId(MetastoreClientUtils.getCatalogId(conf))
                .withWarehouse(new Warehouse(conf))
                .withClientFactory(getGlueClientFactory(conf))
                .build();

    }

    private static GlueClientFactory getGlueClientFactory(HiveConf conf) {
        return () -> AWSGlueClientBuilder.standard()
                .withCredentials(getAwsCredentialsProvider(conf))
                .withEndpointConfiguration(getEndpointConfiguration(conf))
                .withClientConfiguration(buildClientConfiguration(conf)).build();
    }

    private static AWSCredentialsProvider getAwsCredentialsProvider(HiveConf conf) {
        Class<? extends AWSCredentialsProviderFactory> providerFactoryClass = conf
                .getClass("aws.catalog.credentials.provider.factory.class",
                        DefaultAWSCredentialsProviderFactory.class)
                .asSubclass(AWSCredentialsProviderFactory.class);
        AWSCredentialsProviderFactory provider = ReflectionUtils.newInstance(providerFactoryClass, conf);
        return provider.buildAWSCredentialsProvider(conf);
    }

    private static AwsClientBuilder.EndpointConfiguration getEndpointConfiguration(HiveConf conf) {
        return new AwsClientBuilder.EndpointConfiguration(getEndpoint(conf), getRegion(conf));
    }

    private static String getEndpoint(HiveConf conf) {
        return getProperty("aws.glue.endpoint", conf);
    }

    private static String getRegion(HiveConf conf) {
        return getProperty("aws.region", conf);
    }

    private static String getProperty(String propertyName, HiveConf conf) {
        return Strings.isNullOrEmpty(System.getProperty(propertyName)) ? conf.get(propertyName) : System.getProperty(propertyName);
    }

    private static ClientConfiguration buildClientConfiguration(HiveConf hiveConf) {
        return new ClientConfiguration()
                .withUserAgent(createUserAgent())
                .withMaxErrorRetry(hiveConf.getInt("aws.glue.max-error-retries", 5))
                .withMaxConnections(hiveConf.getInt("aws.glue.max-connections", 50))
                .withConnectionTimeout(hiveConf.getInt("aws.glue.connection-timeout", 10000))
                .withSocketTimeout(hiveConf.getInt("aws.glue.socket-timeout", 50000));
    }

    private static String createUserAgent() {
        try {
            String ugi = UserGroupInformation.getCurrentUser().getUserName();
            return "ugi=" + ugi;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
