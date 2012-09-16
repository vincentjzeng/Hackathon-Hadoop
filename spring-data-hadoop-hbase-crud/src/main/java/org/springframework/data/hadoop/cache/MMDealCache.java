package org.springframework.data.hadoop.cache;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.springframework.data.hadoop.dao.CashPosition;
import org.springframework.data.hadoop.dao.MMDeal;

public class MMDealCache {

	private HashMap <String, List <MMDeal>> mmCache = new HashMap<String, List <MMDeal>>();
	
	private static MMDealCache mmDealCache = new MMDealCache();
	
	private MMDealCache(){
	}
	
	public static MMDealCache getInstance(){
		return mmDealCache;
	}
	
	public void updateCache(String key, List <MMDeal> cpList){
		
		mmCache.put(key, cpList);
	}
	
	public List <MMDeal> get(String key) {
		return mmCache.get(key);
	}
	
	public HashMap <String, List <MMDeal>>  getAll(){
		return mmCache;
	}
	
	public BigDecimal getAmountFromCache (String clientId, Date date, String ccyCode){
		
		List <MMDeal> mmList = mmCache.get(clientId);
		BigDecimal mmAmount = new BigDecimal(0);
		for(MMDeal mm : mmList){
			if (date.equals(mm.getMaturityDate()) 
					&& ccyCode.equals(mm.getCcyCode())){
				mmAmount = mmAmount.add(mm.getInterestAmount()) ;
			}
		}
		
		return mmAmount;
		
	}
	
}
