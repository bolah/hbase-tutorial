/**
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
package org.apache.hbase.tutorial;

import static org.apache.hadoop.hbase.client.MetricsConnection.CLIENT_SIDE_METRICS_ENABLED_KEY;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestClientPushback {

	private static final Log LOG = LogFactory.getLog(TestClientPushback.class);
	private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

	private static final TableName tableName = TableName.valueOf("client-pushback");
	private static final byte[] family = Bytes.toBytes("f");
	private static final byte[] qualifier = Bytes.toBytes("q");
	private static final long flushSizeBytes = 1024;

	@BeforeClass
	public static void setupCluster() throws Exception {
		Configuration conf = UTIL.getConfiguration();
		// turn the memstore size way down so we don't need to write a lot to
		// see changes in memstore
		// load
		conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, flushSizeBytes);
		// ensure we block the flushes when we are double that flushsize
		conf.setLong(HConstants.HREGION_MEMSTORE_BLOCK_MULTIPLIER,
				HConstants.DEFAULT_HREGION_MEMSTORE_BLOCK_MULTIPLIER);
		conf.setBoolean(CLIENT_SIDE_METRICS_ENABLED_KEY, true);
		UTIL.startMiniCluster(1);
		UTIL.createTable(tableName, family);
	}

	@AfterClass
	public static void teardownCluster() throws Exception {
		UTIL.shutdownMiniCluster();
	}

	public void test_PutOneRow_RowIsStored() throws Exception {
		Configuration conf = UTIL.getConfiguration();

		ClusterConnection conn = (ClusterConnection) ConnectionFactory.createConnection(conf);
		Table table = conn.getTable(tableName);
		HRegionServer rs = UTIL.getHBaseCluster().getRegionServer(0);
		// Region region = rs.getOnlineRegions(tableName).get(0);

		LOG.debug("Writing some data to " + tableName);
		// write some data
		Put p = new Put(Bytes.toBytes("key"));
		p.addColumn(family, qualifier, Bytes.toBytes("value1"));
		table.put(p);

		Result result = table.get(new Get(Bytes.toBytes("key")));
		Assert.assertEquals(1, result.size());
	}
	
	public void test_PutLotOfRowsWithSalting_Nohotspotting() throws Exception {
		Configuration conf = UTIL.getConfiguration();

		ClusterConnection conn = (ClusterConnection) ConnectionFactory.createConnection(conf);
		conn.getStatisticsTracker().getStats(server)
		Table table = conn.getTable(tableName);
		HRegionServer rs = UTIL.getHBaseCluster().getRegionServer(0);
		// Region region = rs.getOnlineRegions(tableName).get(0);

		LOG.debug("Writing some data to " + tableName);
		// write some data
		Put p = new Put(Bytes.toBytes("key"));
		p.addColumn(family, qualifier, Bytes.toBytes("value1"));
		table.put(p);

		Result result = table.get(new Get(Bytes.toBytes("key")));
		Assert.assertEquals(1, result.size());
	}
}
