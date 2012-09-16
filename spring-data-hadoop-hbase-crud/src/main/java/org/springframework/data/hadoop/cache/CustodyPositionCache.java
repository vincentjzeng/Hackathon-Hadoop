package org.springframework.data.hadoop.cache;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.springframework.data.hadoop.dao.CustodyPosition;
import org.springframework.data.hadoop.dao.CustodyTrade;

public class CustodyPositionCache  implements Serializable{

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
	
	public BigDecimal getAmountFromCache (String clientId, Date date, String ccyCode){
		
		List <CustodyPosition> cpList = cpCache.get(clientId);
		BigDecimal cpAmount = new BigDecimal(0);
		for(CustodyPosition cp : cpList){
			if (date.equals(cp.getDate())){
				
				cpAmount = cpAmount.add(cp.getTradedQuantity()) ;
			}
		}
		
		return cpAmount;
		
	}
}
