package org.apache.hbase.tutorial;

import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class EmbeddedEntityTutorial {
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
	public void testEmbeddedEntity() {
		Person testPerson = getTestData();
		persistTestData(testPerson);
		
		Person queriedTestData = queryTestData(testPerson.name);
		Assert.assertEquals(2, queriedTestData.emailAddresses.size());
		Assert.assertEquals("first@first.com", queriedTestData.emailAddresses.get(0).emailAddress);
		Assert.assertEquals("second@second.com", queriedTestData.emailAddresses.get(1).emailAddress);

	}
	
	private Person queryTestData(String name) {
		// TODO you have to implement this
		return null;
	}

	private void persistTestData(Person testPerson) {
		// TODO you have to implement this
	}

	private Person getTestData() {
		Person person = new Person();
		person.name = "testName";
		person.emailAddresses.add(new EmailAddress("first@first.com"));
		person.emailAddresses.add(new EmailAddress("second@second.com"));
		
		return person;
	}
	
	private static class Person {
		public String name;
		public List<EmailAddress> emailAddresses = new LinkedList<>();
	}
	
	private static class EmailAddress {
		public String emailAddress;
		
		public EmailAddress(String address) {
			emailAddress = address;
		}
	}
}
