package com.qst.hdfs;

import org.junit.Test;

public class TestSingleton {
	
	@Test
	public void testSingleton(){
		
		Garbage box = Garbage.getInstance();
		
		System.out.println(box.toString());
		System.out.println(Garbage.getInstance());
		System.out.println(Garbage.getInstance());
		System.out.println(Garbage.getInstance());
		System.out.println(Garbage.getInstance());
		
	}
	
	public static void main(String[] args) {
		
		new Thread(){
			public void run(){
				Garbage box = Garbage.getInstance();
				System.out.println(box.toString());
				
			}
			
		}.start();
		
		new Thread(){
			public void run(){
				Garbage box = Garbage.getInstance();
				System.out.println(box.toString());
			}
		}.start();
		
		
	}
	
	
	
	
	

}
