package org.apache.hbase.tutorial;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CounterTutorial  {
	private static final Log LOG = LogFactory.getLog(CounterTutorial.class);
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
	public void testCounter() throws IOException {
		Put put = new Put(Bytes.toBytes("key"));
		put.addColumn(fixture.getColumnFamily(), fixture.getColumnQualifier(), Bytes.toBytes(1l));
		fixture.findTestTable().put(put);
		
		 // vv IncrementMultipleExample
	    Increment increment = new Increment(Bytes.toBytes("key"));

	    increment.addColumn(fixture.getColumnFamily(), fixture.getColumnQualifier(), 1);
	    
	    Table table = fixture.findTestTable();
	    table.increment(increment);
	   
	    Result result = table.get(new Get(Bytes.toBytes("key")));
		Assert.assertEquals(2l, Bytes.toLong(result.getValue(fixture.getColumnFamily(), fixture.getColumnQualifier())));
	}

}
