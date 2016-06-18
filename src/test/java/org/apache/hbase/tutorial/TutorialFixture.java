package org.apache.hbase.tutorial;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

public final class TutorialFixture {
	private static final TableName TABLE_NAME = TableName.valueOf("test-table");
	private static final String COLUMN_FAMILY = "test-column-family";
	private static final String COLUMN_QUALIFIER = "test-column-name";

	protected HBaseTestingUtility hBaseTestingUtility = new HBaseTestingUtility();

	public void setupCluster(byte[][] splits) throws Exception {
		hBaseTestingUtility.startMiniCluster(1);

		if (splits != null) {
			hBaseTestingUtility.createTable(getTableName(), getColumnFamily(), splits);
		} else {
			hBaseTestingUtility.createTable(getTableName(), getColumnFamily());
		}
	}
	
	public void setupCluster() throws Exception {
		setupCluster(null);
	}

	public void teardownCluster() throws Exception {
		hBaseTestingUtility.shutdownMiniCluster();
	}
	
	public List<HRegion> getRegions() {
		return hBaseTestingUtility.getHBaseCluster().getRegions(getTableName());
	}
	
	protected byte[] getColumnFamily() {
		return Bytes.toBytes(COLUMN_FAMILY);
	}

	protected TableName getTableName() {
		return TABLE_NAME;
	}

	protected byte[] getColumnQualifier() {
		return Bytes.toBytes(COLUMN_QUALIFIER);
	}

	protected Table findTestTable() {
		Configuration conf = hBaseTestingUtility.getConfiguration();
		ClusterConnection conn;
		try {
			conn = (ClusterConnection) ConnectionFactory.createConnection(conf);
			Table table = conn.getTable(getTableName());
			return table;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
