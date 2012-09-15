package org.springframework.data.hadoop.cache;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.hadoop.dao.CashPosition;

public class CashPositionCache {
	
	private HashMap <String, List <CashPosition>> cpCache = new HashMap<String, List <CashPosition>>();
	
	private static CashPositionCache cashPositionCache = new CashPositionCache();
	
	private CashPositionCache(){
	}
	
	public static CashPositionCache getInstance(){
		return cashPositionCache;
	}
	
	public void updateCache(String key, List <CashPosition> cpList){
		cpCache.put(key, cpList);
	}
	
	public BigDecimal getAmountFromCache (Date date, String ccyCode){
		
		BigDecimal cashPosition = new BigDecimal(0);
		for(Map.Entry<String, List<CashPosition>> e : cpCache.entrySet()){
			CashPosition cp = (CashPosition)e.getValue();
			if (date.equals(cp.getDate()) 
					&& ccyCode.equals(cp.getCcy())){
					
				cashPosition = cashPosition.add(cp.getAccruedInterestAmount()) ;
			}
		}
		
		return cashPosition;
		
	}

	public List <CashPosition> get(String key) {
		return cpCache.get(key);
	}
	
	public HashMap <String, List <CashPosition>>  getAll(){
		return cpCache;
	}
}
