package org.springframework.data.hadoop.service;


import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import org.springframework.data.hadoop.cache.CashPositionCache;
import org.springframework.data.hadoop.cache.CustodyPositionCache;
import org.springframework.data.hadoop.cache.CustodyTradeCache;
import org.springframework.data.hadoop.cache.EntitlementCache;
import org.springframework.data.hadoop.cache.FXDealCache;
import org.springframework.data.hadoop.cache.MMDealCache;

public class HBaseDataProcessor {
	
	private static final String BONDS = "BONDS";
	
	private static final String STOCKS = "STOCKS";
	
	private static final String FX = "FX";
	
	
	public  Map <String, List> getOverView (String clientId, List <String> prodcutTypeList, String ccyCode, Date startDate, Date endDate){

		Map< Date, Map <String, Amount>> amountDateMap = new HashMap< Date, Map <String, Amount>>();
		
		Map <String, BigDecimal> amountList = new HashMap <String, BigDecimal>();
		Map <String, List>  resultMap = new HashMap <String, List> ();
		List <Date> dateList = new ArrayList <Date> ();
		int dayDiff = endDate.compareTo(startDate);
			for (String productType : prodcutTypeList){
					for (int j = 0; j < dayDiff; j++){
						Calendar cal = Calendar.getInstance();
						cal.setTime(startDate);
						cal.add(Calendar.DAY_OF_YEAR, j);
						Date nextDate = cal.getTime();
						dateList.add(nextDate);
						BigDecimal amount = getCurrentPosition (clientId, productType,  ccyCode,  nextDate);
						amountList.put(productType, amount);
					}
				}

			resultMap.put("items", prodcutTypeList);
			resultMap.put("dateString", dateList);
			
			return resultMap;
		}
	
	
	public BigDecimal getCurrentPosition (String clientId, String productType, String ccyCode, Date date){
		
		BigDecimal amount = new BigDecimal(0);
		
		if (BONDS.equals(productType)){
			
			amount = getBondsAmount(clientId, ccyCode, date);
		} else if (STOCKS.equals(productType)){
			
			amount = getStocksAmount(clientId, ccyCode, date);
			
		} else if (FX.equals(productType)){
			
			amount = getFxAmount(clientId, ccyCode, date);
		} 
		return amount;
	}

	
	private BigDecimal getFxAmount(String clientId, String ccyCode, Date date) {
		// TODO Auto-generated method stub
		
		Calendar cal = Calendar.getInstance();
		Date today = cal.getTime();
		BigDecimal amount = new BigDecimal (0);
		CashPositionCache cpCache = CashPositionCache.getInstance();
		FXDealCache fxDealCache = FXDealCache.getInstance();
		
		MMDealCache mmDealCache = MMDealCache.getInstance();
		if (date.compareTo(today) < 0 || date.compareTo(today) == 0 ){
			amount = cpCache.getAmountFromCache(clientId,date, ccyCode);
		} else {
			BigDecimal fxAmount = fxDealCache.getAmountFromCache(clientId,date, ccyCode);
			BigDecimal mmAmount = mmDealCache.getAmountFromCache(clientId, date, ccyCode);
			amount = fxAmount.add(mmAmount);
		}
		return amount;
		
	}

	private BigDecimal getStocksAmount(String clientId, String ccyCode, Date date) {
		// TODO Auto-generated method stub
		Calendar cal = Calendar.getInstance();
		Date today = cal.getTime();
		BigDecimal amount = new BigDecimal (0);
		EntitlementCache etCache = EntitlementCache.getInstance();
		FXDealCache fxDealCache = FXDealCache.getInstance();
		amount = etCache.getAmountFromCache(clientId,date, ccyCode);
		return amount;
	}

	private BigDecimal getBondsAmount(String clientId, String ccyCode, Date date) {
		// TODO Auto-generated method stub
		Calendar cal = Calendar.getInstance();
		Date today = cal.getTime();
		BigDecimal amount = new BigDecimal (0);
		CustodyPositionCache cpCache = CustodyPositionCache.getInstance();
		CustodyTradeCache ctCache = CustodyTradeCache.getInstance();
		if (date.compareTo(today) < 0 || date.compareTo(today) == 0 ){
			amount = cpCache.getAmountFromCache(clientId, date, ccyCode);
		} else {
			amount = ctCache.getAmountFromCache(clientId, date, ccyCode);
		}
		return amount;
	}
	
}
