package org.springframework.data.hadoop.cache;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.hadoop.dao.FXDeal;

public class FXDealCache {

	private static final String BOOKED = "BOOKED";
	private HashMap <String, List <FXDeal>> fxCache = new HashMap<String, List <FXDeal>>();
	
	private static FXDealCache fxDealCache = new FXDealCache();
	
	private FXDealCache(){
	}
	
	public static FXDealCache getInstance(){
		return fxDealCache;
	}
	
	public void updateCache(String key, List <FXDeal> cpList){
		
		fxCache.put(key, cpList);
	}
	
	public BigDecimal getAmountFromCache (String clientId, Date date, String ccyCode){
		BigDecimal totalAmount = new BigDecimal (0);
		List <FXDeal> fxDealList = fxCache.get(clientId);
		for(FXDeal fxDeal : fxDealList){
			if (date.equals(fxDeal.getSettlementDate()) 
					&& BOOKED.equals(fxDeal.getDealStatus())){
				if (ccyCode.equals(fxDeal.getSellCcyCode())){
					
					totalAmount = totalAmount.add(fxDeal.getSellCcyAmount());
					
				} else if(ccyCode.equals(fxDeal.getBuyCcyCode())){
					
					totalAmount = totalAmount.add(fxDeal.getBuyCcyAmount());
		}
			}
		}
		
		return totalAmount;
	}
	
	public List <FXDeal> get(String key) {
		return fxCache.get(key);
	}
	
	public HashMap <String, List <FXDeal>>  getAll(){
		return fxCache;
	}
}
