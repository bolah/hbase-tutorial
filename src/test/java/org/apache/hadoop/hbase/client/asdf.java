package org.apache.hadoop.hbase.client;

import org.jboss.netty.handler.timeout.ReadTimeoutException;

public class asdf {

	public void method() throws Exception {
			throw new Exception("asdf");
	}

	public void method2() {
		try {
			method();
		} catch (Exception e) {
			throw new ReadTimeoutException(e);
		}
	}
}
