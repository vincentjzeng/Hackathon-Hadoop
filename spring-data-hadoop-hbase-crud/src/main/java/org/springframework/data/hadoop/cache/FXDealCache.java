package org.springframework.data.hadoop.cache;

import java.util.HashMap;
import java.util.List;

import org.springframework.data.hadoop.dao.FXDeal;

public class FXDealCache {

	private HashMap <String, List <FXDeal>> fxCache = new HashMap<String, List <FXDeal>>();
	
	private static FXDealCache fxDealCache;
	
	public static FXDealCache getInstance(){
		
		if (fxDealCache == null){
			
			return new FXDealCache();
		}
		return fxDealCache;
	}
	
	public void updateCache(String key, List <FXDeal> cpList){
		fxCache.put(key, cpList);
	}
	
	public List <FXDeal> get(String key) {
		return fxCache.get(key);
	}
}
