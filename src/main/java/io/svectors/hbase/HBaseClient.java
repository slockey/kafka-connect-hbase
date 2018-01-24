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
package io.svectors.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.svectors.hbase.sink.SinkConnectorException;
import io.svectors.hbase.util.TrackHbaseWrite;

/**
 * @author ravi.magham
 */
public final class HBaseClient implements TrackHbaseWrite{

    final static Logger logger = LoggerFactory.getLogger(HBaseClient.class);

    private long lastWrittenAt = System.currentTimeMillis();
    private String lastWrittenUuid="";
    private final HBaseConnectionFactory connectionFactory;
    private BufferedMutator mutator = null;
    private Connection connection = null;

    public HBaseClient(final HBaseConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public void write(final String tableName, final List<Put> puts) {
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(puts);
        final TableName table = TableName.valueOf(tableName);
        write(table, puts);
    }

    public void write(final TableName table, final List<Put> puts) {
        logger.debug("thread id: "+Thread.currentThread().getId() + " name: " + Thread.currentThread().getName());

        Preconditions.checkNotNull(table);
        Preconditions.checkNotNull(puts);
        try {

            if (!isConnectionOpen()) {
                establishConnection();
            }

            if (mutator == null) {
                mutator = connection.getBufferedMutator(table);
            }

            logger.debug("connection hashcode: " + connection.hashCode());
            logger.debug("mutator hashcode: " + mutator.hashCode());
            logger.debug("puts count: " + puts.size());
            mutator.mutate(puts);
            mutator.flush();
            logger.debug("mutator.flush()");

            for (Put put:puts) {
                if (put!=null) {
                    this.lastWrittenUuid = new String(new String(put.getRow()));
                    this.lastWrittenAt = System.currentTimeMillis();
                }
            }

        } catch (Exception ex) {
            logger.error(String.format(
                    "Failed with a [%s] when writing to table [%s] ",
                    ex.getMessage(), table.getNameAsString()));
            final String errorMsg = String.format(
                    "Failed with a [%s] when writing to table [%s] ",
                    ex.getMessage(), table.getNameAsString());
            throw new SinkConnectorException(errorMsg, ex);
        }
    }

    @Override
    public long getRecentTransactionTime() {
        return this.lastWrittenAt;
    }

    @Override
    public String getLastWrittenUUid() {
        return lastWrittenUuid;
    }

    public void establishConnection() throws IOException {
        this.connection = this.connectionFactory.getConnection();
    }

    public boolean isConnectionOpen() {
        if (connection == null || connection.isAborted() || connection.isClosed()) {
            return false;
        }
        return true;
    }

    public void close() {
        try {
            mutator.close();
            connection.close();
        } catch (IOException e) {
            logger.debug("Exception closing connection OR mutator" + e.getMessage());
            e.printStackTrace();
        }
    }
}
