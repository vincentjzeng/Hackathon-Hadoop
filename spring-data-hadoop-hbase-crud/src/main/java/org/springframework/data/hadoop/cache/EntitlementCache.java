package org.springframework.data.hadoop.cache;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.springframework.data.hadoop.dao.Entitlement;
import org.springframework.data.hadoop.dao.MMDeal;

public class EntitlementCache  implements Serializable{

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
	
	public BigDecimal getAmountFromCache (String clientId, Date date, String ccyCode){
		
		List <Entitlement> etList = etCache.get(clientId);
		BigDecimal etAmount = new BigDecimal(0);
		for(Entitlement et : etList){
			if (date.equals(et.getPayDate()) 
					&& ccyCode.equals(et.getEventCcyCode())){
				etAmount = etAmount.add(et.getNetIncomeAmount()) ;
			}
		}
		
		return etAmount;
		
	}
}
