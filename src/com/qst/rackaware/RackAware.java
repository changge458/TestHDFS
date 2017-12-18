package com.qst.rackaware;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.net.DNSToSwitchMapping;

public class RackAware implements DNSToSwitchMapping {

	public List<String> resolve(List<String> names) {
		//新建list，作为输出
		List<String> list = new ArrayList<String>();
		
		Integer suffix ;
		
		//通过循环，将主机名(IP)进行分类
		for(String name : names){
			/**
			 * 输入192.168.153.201
			 * 截串取201
			 */
			if(name.startsWith("192")){
				suffix = Integer.parseInt(name.substring(name.lastIndexOf(".")+1));
			}
			/**
			 * 输入为s201
			 * 截串取201
			 */
			else{
				suffix = Integer.parseInt(name.substring(1));
			}
			
			/**
			 * 归类
			 */
			if(suffix <= 203){
				list.add("/rack1/");
			}
			else{
				list.add("/rack2/");
			}
		}
		return list;
	}

	public void reloadCachedMappings() {
		// TODO Auto-generated method stub
		
	}

	public void reloadCachedMappings(List<String> names) {
		// TODO Auto-generated method stub
		
	}
	
	

}
