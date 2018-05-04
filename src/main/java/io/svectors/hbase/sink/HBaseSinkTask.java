/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.svectors.hbase.sink;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.svectors.hbase.HBaseClient;
import io.svectors.hbase.HBaseConnectionFactory;
import io.svectors.hbase.config.HBaseSinkConfig;
import io.svectors.hbase.util.HbaseTransactionTimer;
import io.svectors.hbase.util.ToPutFunction;

/**
 * @author ravi.magham
 */
public class HBaseSinkTask extends SinkTask  {
    final static Logger logger = LoggerFactory.getLogger(HBaseSinkTask.class);
    private static final String HBASE_PRODUCER_TOPIC = "hbase.producer.topic";
    private ToPutFunction toPutFunction;
    private HBaseClient hBaseClient;

    @Override
    public String version() {
        return HBaseSinkConnector.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        final HBaseSinkConfig sinkConfig = new HBaseSinkConfig(props);
        Map<String, String> configMap = new TreeMap<String, String>(props);

        logger.debug("HBaseSinkTask.start - thread id: "+Thread.currentThread().getId() + " name: " + Thread.currentThread().getName());

        logger.info("Printing connection configurations:");
        for (Map.Entry entry : configMap.entrySet()) {
            logger.info(entry.getKey() + "......." + entry.getValue());
        }
        sinkConfig.validate();

        String zookeeperQuorum = sinkConfig
                .getString(HBaseSinkConfig.ZOOKEEPER_QUORUM_CONFIG);
        Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum);
        if (sinkConfig.getPropertyValue(HBASE_PRODUCER_TOPIC) != null) {
        configuration.set(HBASE_PRODUCER_TOPIC, sinkConfig.getPropertyValue(HBASE_PRODUCER_TOPIC));
        }
        HBaseConnectionFactory connectionFactory = new HBaseConnectionFactory(
                configuration);
       

        try {
            this.hBaseClient = new HBaseClient(connectionFactory);
            // initialize the persistent hbase connection
            this.hBaseClient.establishConnection();
        } catch (Exception e) {
            logger.error("Unable to establish connection with Hbase.  zoo quorum: " + zookeeperQuorum);
            e.printStackTrace();
        } 

        this.toPutFunction = new ToPutFunction(sinkConfig);
        Timer time = new Timer();
        time.schedule(new HbaseTransactionTimer(this.hBaseClient), 0, 180000); // check in every 3 min
    }

    @Override
    public void put(Collection<SinkRecord> records) {

      //Verify that hbase connection is valid before pulling of the stream
        if (!hBaseClient.isConnectionOpen()) {
            try {
                // re-initialize the persistent hbase connection
                this.hBaseClient.establishConnection();
            } catch (Exception e) {
                logger.error("Unable to re-establish connection with Hbase");
                e.printStackTrace();
            }
        }

        // test connection again before pulling from stream
        if (hBaseClient.isConnectionOpen()) {

            Map<String, List<SinkRecord>> byTopic = records.stream()
                    .collect(groupingBy(SinkRecord::topic));

            Map<String, List<Put>> byTable = byTopic.entrySet().stream()
                    .collect(toMap(Map.Entry::getKey, (e) -> e.getValue().stream()
                            .map(sr -> toPutFunction.apply(sr)).collect(toList())));

            byTable.entrySet().parallelStream().forEach(entry -> {
                hBaseClient.write(entry.getKey(), entry.getValue());
            });
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // NO-OP
    }

    @Override
    public void stop() {
        logger.debug("HBaseSinkTask.stop - thread id: "+Thread.currentThread().getId() + " name: " + Thread.currentThread().getName());
        this.hBaseClient.close();
    }
}
