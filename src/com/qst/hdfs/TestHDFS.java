package com.qst.hdfs;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;


public class TestHDFS {
	/**
	 */
	@Test
	public void testRead() throws Exception{
		
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		//URL url = new URL("http://hadoop.apache.org/releases.html");
		URL url = new URL("hdfs://192.168.153.201:8020/user/centos/core-site.xml");
		URLConnection conn = url.openConnection();
		InputStream is = conn.getInputStream();
		
		int len = 0;
		byte[] buf = new byte[1024];
		
		while( (len = is.read(buf)) != -1){
			System.out.print(new String(buf, 0, len));
		}
		is.close();
	}
	
	/**
	 */
	@Test
	public void testReadByAPI() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.153.201:8020/");
		FileSystem fs = FileSystem.get(conf);
		//
		Path p = new Path("/user/centos/core-site.xml");
		FSDataInputStream fis = fs.open(p);
		
		int len = 0;
		byte[] buf = new byte[1024];
		
		while( (len = fis.read(buf)) != -1){
			System.out.print(new String(buf, 0, len));
		}
		fis.close();
	}
	
	/**
	 */
	@Test
	public void testReadByAPI2() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.153.201:8020/");
		FileSystem fs = FileSystem.get(conf);
		//
		Path p = new Path("/user/centos/core-site.xml");
		FSDataInputStream fis = fs.open(p);
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		IOUtils.copyBytes(fis, baos, 1024);
		
		System.out.println(baos.toString());
		
	}
	/**
	 * API进行文件夹的创建
	 * @throws Exception
	 */
	@Test
	public void mkdir() throws Exception{
		System.setProperty("HADOOP_USER_NAME", "centos");
		//初始化配置文件
		Configuration conf = new Configuration();
		//初始化文件系统
		FileSystem fs = FileSystem.get(conf);
		
		Path p = new Path("/user/centos2/hadoop");
		//创建文件夹
		fs.mkdirs(p);
		
		System.out.println("ok");
		
	}
	/**
	 * API进行文件的创建
	 * @throws Exception
	 */
	@Test
	public void touchFile() throws Exception{
		System.setProperty("HADOOP_USER_NAME", "centos");
		//初始化配置文件
		Configuration conf = new Configuration();
		//初始化文件系统
		FileSystem fs = FileSystem.get(conf);
		
		Path p = new Path("/user/centos/1.txt");
		//得到输出流，并指定创建目标
		FSDataOutputStream fos = fs.create(p);
		//在流中写入数据
		fos.writeBytes("helloworld");
		//
		fos.close();
		
		System.out.println("ok");
		
	}
	/**
	 * API进行文件(夹)的删除
	 * @throws Exception
	 */
	@Test
	public void rmFile() throws Exception{
		System.setProperty("HADOOP_USER_NAME", "centos");
		//初始化配置文件
		Configuration conf = new Configuration();
		//初始化文件系统
		FileSystem fs = FileSystem.get(conf);
		
		Path p = new Path("/user/centos2");
		
		fs.delete(p, true);
		
		System.out.println("ok");
		
	}
	
	/**
	 * API进行追加文件
	 * @throws Exception
	 */
	@Test
	public void appendFile() throws Exception{
		System.setProperty("HADOOP_USER_NAME", "centos");
		//初始化配置文件
		Configuration conf = new Configuration();
		//初始化文件系统
		FileSystem fs = FileSystem.get(conf);
		
		Path p = new Path("/user/centos/1.txt");
		//使用append方法得到输出流
		FSDataOutputStream fos = fs.append(p);
		//追加的输入流
		FileInputStream fis = new java.io.FileInputStream("D:/1.txt");
		//开始拷贝
		IOUtils.copyBytes(fis, fos, 1024);
		
		fis.close();
		fos.close();
		
		System.out.println("ok");
		
	}
	
	/**
	 * API进行上传文件
	 * @throws Exception
	 */
	@Test
	public void putFile() throws Exception{
		System.setProperty("HADOOP_USER_NAME", "centos");
		//初始化配置文件
		Configuration conf = new Configuration();
		//初始化文件系统
		FileSystem fs = FileSystem.get(conf);
		
		Path p = new Path("D:/1.txt");

		Path p2 = new Path("/user/centos/2.txt");
		
		fs.copyFromLocalFile(p, p2);
		
		System.out.println("ok");
		
	}
	
}
