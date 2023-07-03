/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hiscat.flink.prometheus;

import com.hiscat.flink.prometheus.sd.ServiceDiscovery;
import com.hiscat.flink.prometheus.sd.ServiceDiscoveryLoader;
import com.sun.net.httpserver.HttpServer;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.prometheus.AbstractPrometheusReporter;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Iterator;

import static com.hiscat.flink.prometheus.sd.ZookeeperServiceDiscoveryOptions.SD_IDENTIFIER;


/**
 * {@link MetricReporter} that exports {@link Metric Metrics} via Prometheus.
 */
@PublicEvolving
@InstantiateViaFactory(
        factoryClassName = "com.hiscat.flink.prometheus.AtuoServiceDiscoveryPrometheusReporterFactory")
public class AutoServiceDiscoveryPrometheusReporter extends AbstractPrometheusReporter {

    private HTTPServer httpServer;
    private ServiceDiscovery sd;


    @Override
    public void open(MetricConfig config) {
        super.open(config);
        sd = ServiceDiscoveryLoader
                .load(config.getString(SD_IDENTIFIER.key(), SD_IDENTIFIER.defaultValue()), this.getClass().getClassLoader());
        sd.register(getAddress(), config);
    }

    private InetSocketAddress getAddress() {
        try {
            Field server = this.httpServer.getClass().getDeclaredField("server");
            server.setAccessible(true);
            return ((HttpServer) server.get(this.httpServer)).getAddress();
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    AutoServiceDiscoveryPrometheusReporter(Iterator<Integer> ports) {
        while (ports.hasNext()) {
            int port = ports.next();
            try {
                // internally accesses CollectorRegistry.defaultRegistry
                httpServer = new HTTPServer(port);
                log.info("Started PrometheusReporter HTTP server on port {}.", port);
                break;
            } catch (IOException ioe) { // assume port conflict
                log.debug("Could not start PrometheusReporter HTTP server on port {}.", port, ioe);
            }
        }
        if (httpServer == null) {
            throw new RuntimeException(
                    "Could not start PrometheusReporter HTTP server on any configured port. Ports: "
                            + ports);
        }
    }

    @Override
    public void close() {
        if (httpServer != null) {
            httpServer.stop();
        }

        sd.close();

        super.close();
    }
}
