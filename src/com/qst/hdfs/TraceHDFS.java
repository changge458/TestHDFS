package com.qst.hdfs;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class TraceHDFS {
	
	@Test
	public void putFile() throws Exception{
		
		//System.setProperty("HADOOP_USER_NAME", "centos");
		
		Configuration conf = new Configuration();
		
		conf.set("fs.defaultFS", "file:///");
		//获取配置文件，返回分布式文件系统
		FileSystem fs = FileSystem.get(conf);
		
		Path p = new Path("D:/codec/1.txt");
		//首先，通过
		FSDataOutputStream fos = fs.create(p, true);
		
		IOUtils.copyBytes(new FileInputStream("D:/1.txt"), fos, 1024);
		
		fos.close();
		
		System.out.println("ok");
	}
	
	@Test
	public void readFile() throws Exception{
		
		//System.setProperty("HADOOP_USER_NAME", "centos");
		
		Configuration conf = new Configuration();
		
		conf.set("fs.defaultFS", "file:///");
		//获取配置文件，返回分布式文件系统
		FileSystem fs = FileSystem.get(conf);
		
		fs.setVerifyChecksum(false);
		
		Path p = new Path("D:/codec/1.txt");
		//首先，通过
		FSDataInputStream fis =  fs.open(p);
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		
		IOUtils.copyBytes(fis, baos , 1024);
		
		System.out.println(baos.toString());
		
		fis.close();
		baos.close();
		
		System.out.println("ok");
	}

}
