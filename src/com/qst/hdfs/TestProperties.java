package com.qst.hdfs;

import java.io.FileInputStream;
import java.util.Properties;

import org.junit.Test;

public class TestProperties {
	
	@Test
	public void testProp() throws Exception{
		
		Properties prop = new Properties();
		prop.load(new FileInputStream("D:/draw/user.properties"));
		
		System.out.println(prop.getProperty("name"));
		
	}

}
