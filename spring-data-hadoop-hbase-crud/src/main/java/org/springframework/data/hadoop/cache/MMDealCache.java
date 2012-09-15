package org.springframework.data.hadoop.cache;

import java.util.HashMap;
import java.util.List;

import org.springframework.data.hadoop.dao.MMDeal;

public class MMDealCache {

	private HashMap <String, List <MMDeal>> mmCache = new HashMap<String, List <MMDeal>>();
	
	private static MMDealCache mmDealCache = new MMDealCache();
	
	private MMDealCache(){
	}
	
	public static MMDealCache getInstance(){
		return mmDealCache;
	}
	
	public void updateCache(String key, List <MMDeal> cpList){
		
		mmCache.put(key, cpList);
	}
	
	public List <MMDeal> get(String key) {
		return mmCache.get(key);
	}
	
	public HashMap <String, List <MMDeal>>  getAll(){
		return mmCache;
	}
	
}
