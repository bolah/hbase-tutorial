package org.apache.hbase.tutorial;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ScanTutorial {
	private TutorialFixture fixture = new TutorialFixture();

	@Before
	public void setup() throws Exception {
		fixture.setupCluster();
	}

	@After
	public void tearDown() throws Exception {
		fixture.teardownCluster();
	}

	@Test
	public void testScan() throws IOException {
		Put put = new Put(Bytes.toBytes("key1"));
		put.addColumn(fixture.getColumnFamily(), fixture.getColumnQualifier(), Bytes.toBytes(1l));
		fixture.findTestTable().put(put);

		put = new Put(Bytes.toBytes("key2"));
		put.addColumn(fixture.getColumnFamily(), fixture.getColumnQualifier(), Bytes.toBytes(1l));
		fixture.findTestTable().put(put);

		put = new Put(Bytes.toBytes("key3"));
		put.addColumn(fixture.getColumnFamily(), fixture.getColumnQualifier(), Bytes.toBytes(1l));
		fixture.findTestTable().put(put);

		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes("key1"));
		scan.setStopRow(Bytes.toBytes("key3"));

		ResultScanner resultScanner = fixture.findTestTable().getScanner(scan);

		final AtomicInteger count = new AtomicInteger(0);
		resultScanner.forEach(new Consumer<Result>() {

			@Override
			public void accept(Result result) {
				count.incrementAndGet();
			}
		});
		
		Assert.assertEquals(2, count.get());
	}

}
