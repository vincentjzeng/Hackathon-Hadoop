package org.springframework.data.hadoop.cache;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.hadoop.dao.FXDeal;

public class FXDealCache implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -684350633697261534L;
	private static final String BOOKED = "BOOKED";
	private HashMap <String, List <FXDeal>> fxCache = new HashMap<String, List <FXDeal>>();
	
	private static FXDealCache fxDealCache = new FXDealCache();
	
	private FXDealCache(){
	}
	
	public static FXDealCache getInstance(){
		return fxDealCache;
	}
	
	public void initCache(HashMap <String, List <FXDeal>> cache){
		this.fxCache = cache;
	}
	
	public void updateCache(String key, List <FXDeal> cpList){
		fxCache.put(key, cpList);
	}
	
	public BigDecimal getAmountFromCache (Date date, String ccyCode){
		
		BigDecimal totalAmount = new BigDecimal (0);
		for(Map.Entry<String, List<FXDeal>> e : fxCache.entrySet()){
			FXDeal fxDeal = (FXDeal)e.getValue();
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
