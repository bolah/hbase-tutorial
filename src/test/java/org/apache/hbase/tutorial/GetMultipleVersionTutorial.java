package org.apache.hbase.tutorial;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.SplitLogTask.Err;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.jets3t.service.multi.event.UpdateACLEvent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class GetMultipleVersionTutorial {
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
	public void write3VersionWithSameKey_thenQueryTheLastTwo() throws IOException {
		byte[] key = Bytes.toBytes("key");
		Put put = new Put(key);
		put.addColumn(fixture.getColumnFamily(), fixture.getColumnQualifier(), Bytes.toBytes(1l));
		fixture.findTestTable().put(put);

		put = new Put(key);
		put.addColumn(fixture.getColumnFamily(), fixture.getColumnQualifier(), Bytes.toBytes(2l));
		fixture.findTestTable().put(put);
		
		put = new Put(key);
		put.addColumn(fixture.getColumnFamily(), fixture.getColumnQualifier(), Bytes.toBytes(3l));
		fixture.findTestTable().put(put);
		
		Get get = new Get(key);

		Err we need some magic to return 2 rows;
		
		Result result = fixture.findTestTable().get(get);
		
		List<Cell> columnCells = result.getColumnCells(fixture.getColumnFamily(), fixture.getColumnQualifier());
		Assert.assertEquals(2, columnCells.size());
	}
}