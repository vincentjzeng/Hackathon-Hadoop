package org.springframework.data.hadoop.service;


import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.springframework.data.hadoop.cache.CashPositionCache;
import org.springframework.data.hadoop.cache.FXDealCache;

public class HBaseDataProcessor {
	
	private static final String BONDS = "BONDS";
	
	private static final String STOCKS = "STOCKS";
	
	private static final String FX = "FX";
	
	
	public  Map< Date, Map <String, Amount>> getPositionByDate (String clientId, String productType[], String ccyCode, Date startDate, Date endDate){
		
		Map< Date, Map <String, Amount>> amountDateMap = new HashMap< Date, Map <String, Amount>>();
		
		Map <String, Amount> amountProdMap = new HashMap <String, Amount> ();
		
		int dayDiff = endDate.compareTo(startDate);
		if (productType!= null && productType.length != 0){
			for (int i = 0; i < productType.length; i++){
				for (int j = 0; j < dayDiff; j++){
					Calendar cal = Calendar.getInstance();
					cal.setTime(startDate);
					cal.add(Calendar.DAY_OF_YEAR, j);
					Date nextDate = cal.getTime();
					Amount amount = getCurrentPosition (clientId, productType[i],  ccyCode,  nextDate);
					amountProdMap.put(productType[i], amount);
					amountDateMap.put(nextDate, amountProdMap);
				}
			}

		}

		return amountDateMap;
	}
	
	
	public Amount getCurrentPosition (String clientId, String productType, String ccyCode, Date date){
		
		Amount amount = new Amount();
		if (BONDS.equals(productType)){
			
			amount = getBondsAmount(clientId, ccyCode, date);
		} else if (STOCKS.equals(productType)){
			
			amount = getStocksAmount(clientId, ccyCode, date);
			
		} else if (FX.equals(productType)){
			
			amount = getFxAmount(clientId, ccyCode, date);
		} 
		return amount;
	}

	private Amount getFxAmount(String clientId, String ccyCode, Date date) {
		// TODO Auto-generated method stub
		Amount amount = new Amount();
		CashPositionCache cpCache = CashPositionCache.getInstance();
		FXDealCache fxDealCache = FXDealCache.getInstance();
		amount.setCurrentAllAmount(cpCache.getAmountFromCache(date, ccyCode));
		amount.setFocastingAllAmount(fxDealCache.getAmountFromCache(date, ccyCode));
		return amount;
		
	}

	private Amount getStocksAmount(String clientId, String ccyCode, Date date) {
		// TODO Auto-generated method stub
		Amount amount = new Amount();
		CashPositionCache cpCache = CashPositionCache.getInstance();
		FXDealCache fxDealCache = FXDealCache.getInstance();
		amount.setCurrentAllAmount(cpCache.getAmountFromCache(date, ccyCode));
		amount.setFocastingAllAmount(fxDealCache.getAmountFromCache(date, ccyCode));
		return amount;
	}

	private Amount getBondsAmount(String clientId, String ccyCode, Date date) {
		// TODO Auto-generated method stub
		Amount amount = new Amount();
		CashPositionCache cpCache = CashPositionCache.getInstance();
		FXDealCache fxDealCache = FXDealCache.getInstance();
		amount.setCurrentAllAmount(cpCache.getAmountFromCache(date, ccyCode));
		amount.setFocastingAllAmount(fxDealCache.getAmountFromCache(date, ccyCode));
		return amount;
	}
	
}
