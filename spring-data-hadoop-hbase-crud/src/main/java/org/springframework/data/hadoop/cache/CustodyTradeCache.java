package org.springframework.data.hadoop.cache;

import java.util.HashMap;
import java.util.List;

import org.springframework.data.hadoop.dao.CustodyTrade;

public class CustodyTradeCache {

	private HashMap <String, List <CustodyTrade>> ctCache = new HashMap<String, List <CustodyTrade>>();
	
	private static CustodyTradeCache custodyTradeCache = new CustodyTradeCache();
	
	private CustodyTradeCache(){
	}
	
	public static CustodyTradeCache getInstance(){
		return custodyTradeCache;
	}
	
	public void updateCache(String key, List <CustodyTrade> cpList){
		ctCache.put(key, cpList);
	}
	
	public List <CustodyTrade> get(String key) {
		return ctCache.get(key);
	}
	
	public HashMap <String, List <CustodyTrade>>  getAll(){
		return ctCache;
	}
	
}
