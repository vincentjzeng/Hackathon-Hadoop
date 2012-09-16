package org.springframework.data.hadoop.cache;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.springframework.data.hadoop.dao.CustodyTrade;
import org.springframework.data.hadoop.dao.Entitlement;

public class CustodyTradeCache  implements Serializable{

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
	
	public BigDecimal getAmountFromCache (String clientId, Date date, String ccyCode){
		
		List <CustodyTrade> etList = ctCache.get(clientId);
		BigDecimal ctAmount = new BigDecimal(0);
		for(CustodyTrade ct : etList){
			if (date.equals(ct.getValueDate()) 
					&& ccyCode.equals(ct.getTradeCcyCode())){
				ctAmount = ctAmount.add(ct.getTradeAmount()) ;
			}
		}
		
		return ctAmount;
		
	}
	
}
