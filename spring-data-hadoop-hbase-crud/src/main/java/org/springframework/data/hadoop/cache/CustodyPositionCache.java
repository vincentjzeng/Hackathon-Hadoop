package org.springframework.data.hadoop.cache;

import java.util.HashMap;
import java.util.List;

import org.springframework.data.hadoop.dao.CustodyPosition;

public class CustodyPositionCache {

	private HashMap <String, List <CustodyPosition>> cpCache = new HashMap<String, List <CustodyPosition>>();
	
	private static CustodyPositionCache custodyPositionCache = new CustodyPositionCache();
	
	private CustodyPositionCache(){
	}
	
	public static CustodyPositionCache getInstance(){
		return custodyPositionCache;
	}
	
	public void updateCache(String key, List <CustodyPosition> cpList){
		
		cpCache.put(key, cpList);
	}
	
	public List <CustodyPosition> get(String key) {
		return cpCache.get(key);
	}
	
	public HashMap <String, List <CustodyPosition>>  getAll(){
		return cpCache;
	}
}
