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
    //public final class HBaseClient implements TrackHbaseWrite {
   long lastWrittenAt = System.currentTimeMillis();
   String lastWrittenUuid="";
    final static Logger logger = LoggerFactory.getLogger(HBaseClient.class);

    private final HBaseConnectionFactory connectionFactory;

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
        Preconditions.checkNotNull(table);
        Preconditions.checkNotNull(puts);
        try {
            final Connection connection = establishConnection();
            final BufferedMutator mutator = connection
                    .getBufferedMutator(table);
            mutator.mutate(puts);
            mutator.flush();
            
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

    public Connection establishConnection() throws Exception {
        final Connection connection = this.connectionFactory.getConnection();
        return connection;
    }

    @Override
    public long getRecentTransactionTime() {
        return this.lastWrittenAt;
    }

    @Override
    public String getLastWrittenUUid() {
        // TODO Auto-generated method stub
        return lastWrittenUuid;
    }
}


