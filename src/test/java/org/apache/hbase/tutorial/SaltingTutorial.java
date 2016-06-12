package org.apache.hbase.tutorial;

import static org.apache.hadoop.hbase.client.MetricsConnection.CLIENT_SIDE_METRICS_ENABLED_KEY;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
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
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hbase.tutorial.util.Statistics;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class SaltingTutorial {
	private static final int EMPTY_REGION_SIZE = 2360;
	private static final Log LOG = LogFactory.getLog(SaltingTutorial.class);
	private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

	private static final TableName TABLE_NAME = TableName.valueOf("test-table");
	private static final byte[] COLUMN_FAMILY = Bytes.toBytes("test-column-family");
	private static final byte[] COLUMN_QUALIFIER = Bytes.toBytes("test-column-name");

	@BeforeClass
	public static void setupCluster() throws Exception {
		UTIL.startMiniCluster(1);
		byte[][] splits = new byte[][] { Bytes.toBytes("000000000000000000000000000000000"),
				Bytes.toBytes("100000000000000000000000000000000"), Bytes.toBytes("200000000000000000000000000000000"),
				Bytes.toBytes("300000000000000000000000000000000"), Bytes.toBytes("400000000000000000000000000000000"),
				Bytes.toBytes("500000000000000000000000000000000"), Bytes.toBytes("600000000000000000000000000000000"),
				Bytes.toBytes("700000000000000000000000000000000"), Bytes.toBytes("800000000000000000000000000000000"),
				Bytes.toBytes("900000000000000000000000000000000") };

		UTIL.createTable(TABLE_NAME, COLUMN_FAMILY, splits);
	}

	@AfterClass
	public static void teardownCluster() throws Exception {
		UTIL.shutdownMiniCluster();
	}

	@Test
	public void test_YourSaltingMethodImplementation() throws Exception {
		Table table = findTestTable();

		insertTestData(table, LocalDateTime.now(), 1000);

		Statistics statistics = collectStatistics();
		double expectedDeviation = 40_000;
		Assert.assertTrue(
				"Expected deviation " + expectedDeviation + " <" + " actual deviation " + statistics.getStdDev(),
				statistics.getStdDev() < expectedDeviation);
	}

	private Statistics collectStatistics() {
		List<HRegion> regions = UTIL.getHBaseCluster().getRegions(TABLE_NAME);
		List<Double> data = new ArrayList<>();

		for (HRegion hRegion : regions) {
			long realHeapSize = hRegion.heapSize() - EMPTY_REGION_SIZE;
			LOG.info(hRegion + " heap size: " + realHeapSize);
			data.add((double) realHeapSize);
		}

		Double[] dataList = data.toArray(new Double[data.size()]);
		double[] dataListPrimitive = ArrayUtils.toPrimitive(dataList);

		return new Statistics(dataListPrimitive);
	}

	private Table findTestTable() throws IOException {
		Configuration conf = UTIL.getConfiguration();
		ClusterConnection conn = (ClusterConnection) ConnectionFactory.createConnection(conf);
		Table table = conn.getTable(TABLE_NAME);
		return table;
	}

	private void insertTestData(Table table, LocalDateTime localDateTime, int numberOfRows) throws IOException {
		LOG.debug(String.format("Writing %s rows to table ", numberOfRows));

		for (int i = 0; i < numberOfRows; i++) {
			localDateTime = localDateTime.plus(1, ChronoUnit.SECONDS);
			String saltedKey = createKey(localDateTime);
			System.out.println(saltedKey);
			Put p = new Put(Bytes.toBytes(saltedKey));
			p.addColumn(COLUMN_FAMILY, COLUMN_QUALIFIER, Bytes.toBytes("value1"));
			table.put(p);
		}
	}
	
	private String createKey(LocalDateTime key) {
		return key.toString();
//		return rightPadZeros(Math.abs(key.hashCode()) + "_" + key.toString(), 34);
	}

	public static String rightPadZeros(String str, int num) {
		return String.format("%1$-" + num + "s", str).replace(' ', '0');
	}
	
	
}