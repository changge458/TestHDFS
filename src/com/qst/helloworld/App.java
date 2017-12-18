package com.qst.helloworld;

import java.io.FileInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

/**
 * Hello world!
 *
 */
public class App {

	@Test
	public void putFile() throws Exception{
		System.setProperty("HADOOP_USER_NAME","centos");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream fos = fs.create(new Path("/user/centos.1.txt"), true, 1024, (short)2, 134217728);
		IOUtils.copyBytes(new FileInputStream("D:/1.txt"), fos, 1024);
		System.out.println("ok");
	} 
	
}
