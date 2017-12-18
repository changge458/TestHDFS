package com.qst.hdfs;

public class Garbage {

	private static Garbage instance = null;
	private static Object lock = new Object();

	private Garbage() {
	}

	public static Garbage getInstance() {
		if(instance == null ){
			
			synchronized (lock) {
				if (instance == null) {
					instance = new Garbage();
				}
				return instance;
			}
		}
		return instance;
	}
}
