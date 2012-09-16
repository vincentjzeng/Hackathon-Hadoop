package org.springframework.data.hadoop.cache;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.hadoop.dao.CashPosition;
import org.springframework.data.hadoop.dao.MMDeal;

public class CashPositionCache  implements Serializable{
	
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
	
	public BigDecimal getAmountFromCache (String clientId, Date date, String ccyCode){
		
		List <CashPosition> cpList = cpCache.get(clientId);
		BigDecimal cashPosition = new BigDecimal(0);
		for(CashPosition cp : cpList){
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
	
	
	public void initCache(HashMap <String, List <CashPosition>> cache){
		this.cpCache = cache;
	}
}
