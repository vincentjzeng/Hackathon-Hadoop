package org.springframework.data.hadoop.cache;

import java.util.HashMap;
import java.util.List;

import org.springframework.data.hadoop.dao.CashPosition;

public class CashPositionCache {
	
	private HashMap <String, List <CashPosition>> cpCache = new HashMap<String, List <CashPosition>>();
	
	private static CashPositionCache cashPositionCache;
	
	public static CashPositionCache getInstance(){
		
		if (cashPositionCache == null){
			
			return new CashPositionCache();
		}
		return cashPositionCache;
	}
	
	public void updateCache(String key, List <CashPosition> cpList){
		
		
	}

}
