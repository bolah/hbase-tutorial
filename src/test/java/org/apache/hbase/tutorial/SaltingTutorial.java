package org.apache.hbase.tutorial;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.tutorial.util.Statistics;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SaltingTutorial {
	private static final int EMPTY_REGION_SIZE = 2360;
	private static final Log LOG = LogFactory.getLog(SaltingTutorial.class);
	private TutorialFixture fixture = new TutorialFixture();
	
	@Before
	public void setup() throws Exception {
		byte[][] testTableSplits = new byte[][] { Bytes.toBytes("000000000000000000000000000000000"),
			Bytes.toBytes("100000000000000000000000000000000"),
			Bytes.toBytes("200000000000000000000000000000000"),
			Bytes.toBytes("300000000000000000000000000000000"), 
			Bytes.toBytes("400000000000000000000000000000000"),
			Bytes.toBytes("500000000000000000000000000000000"),
			Bytes.toBytes("600000000000000000000000000000000"),
			Bytes.toBytes("700000000000000000000000000000000"),
			Bytes.toBytes("800000000000000000000000000000000"),
			Bytes.toBytes("900000000000000000000000000000000") };
			
		fixture.setupCluster(testTableSplits);
	}
	
	@After
	public void tearDown() throws Exception {
		fixture.teardownCluster();
	}
	

	@Test
	public void test_YourSaltingMethodImplementation() throws Exception {
		Table table = fixture.findTestTable();

		insertTestData(table, LocalDateTime.now(), 1000);

		Statistics statistics = collectStatistics();
		double expectedDeviation = 40_000;
		Assert.assertTrue(
				"Expected deviation " + expectedDeviation + " <" + " actual deviation " + statistics.getStdDev(),
				statistics.getStdDev() < expectedDeviation);
	}

	private Statistics collectStatistics() {
		List<HRegion> regions = fixture.getRegions();
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

	private void insertTestData(Table table, LocalDateTime localDateTime, int numberOfRows) throws IOException {
		LOG.info(String.format("Writing %s rows to table ", numberOfRows));

		for (int i = 0; i < numberOfRows; i++) {
			localDateTime = localDateTime.plus(1, ChronoUnit.SECONDS);
			String saltedKey = createKey(localDateTime);
			LOG.debug(saltedKey);
			Put p = new Put(Bytes.toBytes(saltedKey));
			p.addColumn(fixture.getColumnFamily(), fixture.getColumnQualifier(), Bytes.toBytes("value1"));
			table.put(p);
		}
	}
	
	private String createKey(LocalDateTime key) {
//		return key.toString();
		return rightPadZeros(Math.abs(key.hashCode()) + "_" + key.toString(), 34);
	}

	public static String rightPadZeros(String str, int num) {
		return String.format("%1$-" + num + "s", str).replace(' ', '0');
	}

}