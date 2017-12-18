package com.qst.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

public class TestList {
	
	public static void main(String[] args) {
		Path p = new Path("/");
		testFile(p);
	}
	
	public static void testFile(Path p){
		
		try {
			//初始化配置文件
			Configuration conf = new Configuration();
			//初始化文件系统
			FileSystem fs = FileSystem.get(conf);
			
			
			//使用listStatus方法，取得当前目录下所有文件状态
			FileStatus[] statuses =  fs.listStatus(p);
			
			//通过增强for取得所有文件(夹)的路径
			for(FileStatus status : statuses){
				//如果是文件夹，则打印出路径，然后更新path
				if(status.isDirectory()){
					Path dirPath = status.getPath();
					System.out.println("文件夹路径："+dirPath);
					testFile(dirPath);
				}
				else{
					Path filePath = status.getPath();
					System.out.println("文件路径："+filePath);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}
	
	@Test
	public void testFile2() throws Exception{
		//初始化配置文件
		Configuration conf = new Configuration();
		//初始化文件系统
		FileSystem fs = FileSystem.get(conf);
		
		Path p = new Path("/");
		//通过listFiles方法，取得文件名称
		RemoteIterator<LocatedFileStatus> status = fs.listFiles(p, true);
		while(status.hasNext()){
			System.out.println(status.next().getPath());
		}
	}

}
