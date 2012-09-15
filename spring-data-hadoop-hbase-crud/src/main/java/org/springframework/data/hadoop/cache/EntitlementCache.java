package org.springframework.data.hadoop.cache;

import java.util.HashMap;
import java.util.List;

import org.springframework.data.hadoop.dao.Entitlement;

public class EntitlementCache {

private HashMap <String, List <Entitlement>> etCache = new HashMap<String, List <Entitlement>>();
	
	private static EntitlementCache entitlementCache = new EntitlementCache();
	
	private EntitlementCache(){
	}
	
	public static EntitlementCache getInstance(){
		return entitlementCache;
	}
	
	public void updateCache(String key, List <Entitlement> cpList){
		
		etCache.put(key, cpList);
	}
	
	public List <Entitlement> get(String key) {
		return etCache.get(key);
	}
	
	public HashMap <String, List <Entitlement>>  getAll(){
		return etCache;
	}
}
