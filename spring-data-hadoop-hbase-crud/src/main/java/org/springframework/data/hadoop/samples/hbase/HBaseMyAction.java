/*
 * Copyright 2011-2012 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.hadoop.samples.hbase;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.cache.CashPositionCache;
import org.springframework.data.hadoop.cache.CustodyPositionCache;
import org.springframework.data.hadoop.cache.CustodyTradeCache;
import org.springframework.data.hadoop.cache.EntitlementCache;
import org.springframework.data.hadoop.cache.FXDealCache;
import org.springframework.data.hadoop.cache.FileUnits;
import org.springframework.data.hadoop.cache.MMDealCache;
import org.springframework.data.hadoop.dao.CashPosition;
import org.springframework.data.hadoop.dao.CustodyPosition;
import org.springframework.data.hadoop.dao.CustodyTrade;
import org.springframework.data.hadoop.dao.Entitlement;
import org.springframework.data.hadoop.dao.FXDeal;
import org.springframework.data.hadoop.dao.MMDeal;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.ResultsExtractor;
import org.springframework.data.hadoop.hbase.TableCallback;
import org.springframework.stereotype.Component;

/**
 * Demo class counting occurrances in HBase through SHDP HBaseTemplate. 
 * 
 * @author Costin Leau
 * @author Jarred Li
 */
@Component
public class HBaseMyAction {

	@Autowired
	private Configuration hbaseConfiguration;

	@Inject
	private HbaseTemplate t;

	private static String columnFamilyName = "cf";
	private static String cellValue = "http://blog.springsource.org/2012/02/29/introducing-spring-hadoop/";
	
	private Map<String, List<String>> clientIdToFxmmCode = new HashMap<String, List<String>>();
	private Map<String, List<String>> clientIdToCashAccountNumber = new HashMap<String, List<String>>();
	private Map<String, List<String>> clientIdToSecurityAccountNumber = new HashMap<String, List<String>>();
	
	/**
	 * Main entry point.
	 */
	@PostConstruct
	public void run() throws Exception {
		System.out.println("Application starting...");

		//4. get 1 column data
		//readData();

		//5. scan data

		scanClientHierarchy("11");
		//readClientHierarchy();

//		scanCorporateEventEntitlements();
//		scanCustodyPosition();
//		scanCustodyTrade();

		System.out.println("Application started");
	}

	/**
	 * Read data from table.
	 * 
	 */
	private void readClientHierarchy() {
		// replace with get 
		System.out.println(t.execute("g11_client_hierarchy", new TableCallback<Long>() {
			public Long doInTable(HTable table) throws Throwable {
				//Get get = new Get(Bytes.toBytes("11UKHUB73288764"));
				Get get = new Get(Bytes.toBytes("11UKHUB73288764"));
				Result r = table.get(get);
				
				List<String> fxmmCodes = new ArrayList<String>();
				List<String> cashAccountNumbers = new ArrayList<String>();
				List<String> securityAccountNumbers = new ArrayList<String>();
				
				StringBuilder sb = new StringBuilder();
				String clientId = Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("client_id")));
				sb.append(clientId).append(",");
				sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("fxmm_code")))).append(",");
				sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("security_account_number")))).append(",");
				byte[] sourceSystemCountryCode = r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("source_system_country_code"));
				sb.append(sourceSystemCountryCode != null ? Bytes.toString(sourceSystemCountryCode) : null).append(",");
				byte[] sourceSystemCode = r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Source_system_code"));
				sb.append(sourceSystemCode != null ? Bytes.toString(sourceSystemCode) : sourceSystemCode).append(",");
				sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("calypso_counterpty_id")))).append(",");
				sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("cash_account_number"))));
				System.out.println(sb.toString());
				
				String fxmmCode = Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("fxmm_code")));
				if(fxmmCode != null) {
					fxmmCodes.add(fxmmCode);
				}
				clientIdToFxmmCode.put(clientId,fxmmCodes);
				
				String cashAccountNumber = Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("cash_account_number")));
				if(cashAccountNumber != null) {
					cashAccountNumbers.add(cashAccountNumber);
				}
				clientIdToCashAccountNumber.put(clientId, cashAccountNumbers);
				
				String securityAccountNumber = Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("security_account_number")));
				if(securityAccountNumber != null) {
					securityAccountNumbers.add(securityAccountNumber);
				}
				clientIdToSecurityAccountNumber.put(clientId, securityAccountNumbers);
				
				System.out.println("Initialized clientIdToFxmmCode - " + fxmmCodes.size());
				System.out.println("Initialized clientIdToCashAccountNumber - " + cashAccountNumbers.size());
				System.out.println("Initialized clientIdToSecurityAccountNumber - " + securityAccountNumbers.size());
				
				return r.getRow() != null ? Bytes.toLong(r.getRow()) : -1L;
			}
		}));
	}
	

	/**
	 * Scan table data.
	 * 
	 */

	private void scanClientHierarchy(String id) {
		List<Filter> filters = new ArrayList<Filter>();
		Filter clientIdFilter = new SingleColumnValueFilter(Bytes.toBytes(columnFamilyName), Bytes.toBytes("client_id"), CompareOp.EQUAL, Bytes.toBytes(id));
		filters.add(clientIdFilter);
		FilterList filterList1 = new FilterList(filters);
		
		Scan scan = new Scan();
		scan.setFilter(filterList1);
		
		t.find("g11_client_hierarchy", scan, new ResultsExtractor<String>() {

			public String extractData(ResultScanner scanner) throws Exception {
				Iterator<Result> i = scanner.iterator();
				System.out.println("Client ID, FXMM code, Security Account Number, Source System Country code, Source System Code, Calypso Counterparty ID, Cash Account Number\n");
				
				List<String> fxmmCodes = new ArrayList<String>();
				List<String> cashAccountNumbers = new ArrayList<String>();
				List<String> securityAccountNumbers = new ArrayList<String>();
				
				while(i.hasNext()){
					StringBuilder sb = new StringBuilder();
					Result r = i.next();
					
					System.out.println("Row key - " + Bytes.toString(r.getRow()));
					
					String clientId = Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("client_id")));
					sb.append(clientId).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("fxmm_code")))).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("security_account_number")))).append(",");
					byte[] sourceSystemCountryCode = r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("source_system_country_code"));
					sb.append(sourceSystemCountryCode != null ? Bytes.toString(sourceSystemCountryCode) : null).append(",");
					byte[] sourceSystemCode = r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Source_system_code"));
					sb.append(sourceSystemCode != null ? Bytes.toString(sourceSystemCode) : sourceSystemCode).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("calypso_counterpty_id")))).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("cash_account_number"))));
					System.out.println(sb.toString());
					
					String fxmmCode = Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("fxmm_code")));
					if(fxmmCode != null) {
						fxmmCodes.add(fxmmCode);
					}
					clientIdToFxmmCode.put(clientId,fxmmCodes);
					
					String cashAccountNumber = Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("cash_account_number")));
					if(cashAccountNumber != null) {
						cashAccountNumbers.add(cashAccountNumber);
					}
					clientIdToCashAccountNumber.put(clientId, cashAccountNumbers);
					
					String securityAccountNumber = Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("security_account_number")));
					if(securityAccountNumber != null) {
						securityAccountNumbers.add(securityAccountNumber);
					}
					clientIdToSecurityAccountNumber.put(clientId, securityAccountNumbers);
				}
				System.out.println("Initialized clientIdToFxmmCode - " + fxmmCodes.size());
				System.out.println("Initialized clientIdToCashAccountNumber - " + cashAccountNumbers.size());
				System.out.println("Initialized clientIdToSecurityAccountNumber - " + securityAccountNumbers.size());
				return null;
			}
		});
		
		long t1 = System.currentTimeMillis();
		
		List<String> fxmmCodes = clientIdToFxmmCode.get(id);
		System.out.println(fxmmCodes);
		scanFxDeal(id, fxmmCodes);
		
		List<String> portfolioCodes = clientIdToFxmmCode.get(id);
		System.out.println(portfolioCodes);
		scanMmDeal(id, portfolioCodes);
		
		/*
		List<String> cashAccountNumbers = clientIdToCashAccountNumber.get(id);
		System.out.println(cashAccountNumbers);
		scanCashPosition(id, cashAccountNumbers);
		
		List<String> securityAccountNumbers = clientIdToSecurityAccountNumber.get(id);
		System.out.println(securityAccountNumbers);
		for(String securityAccountNumber : securityAccountNumbers){
			if(!securityAccountNumber.equals("\\N")){
				System.out.println("Scaning corporate event entitlements for : " + securityAccountNumber);
				scanCorporateEventEntitlements(securityAccountNumber);
			}
		}
		
		for(String securityAccountNumber : securityAccountNumbers){
			if(!securityAccountNumber.equals("\\N")){
				System.out.println("Scaning custody position for : " + securityAccountNumber);
				scanCustodyPosition(securityAccountNumber);
			}
		}
		
		for(String securityAccountNumber : securityAccountNumbers){
			if(!securityAccountNumber.equals("\\N")){
				System.out.println("Scaning custody trade for : " + securityAccountNumber);
				scanCustodyTrade(securityAccountNumber);
			}
		}
		*/
		
		long t2 = System.currentTimeMillis();
		System.out.println("Processing time - " +  (t2 - t1)/1000);
	}
	
	private void scanFxDeal(String clientId, List<String> cptyCodes) {
		FXDealCache cache = FXDealCache.getInstance();
		String dataFile = "fxCache.dat";
		
		if(FileUnits.isFileExist(dataFile)){
			cache.initCache((HashMap <String, List <FXDeal>>)FileUnits.readFileToCache(dataFile));
			System.out.println("Read file to init cache");
			List<FXDeal> fxCache = cache.get(clientId);
			if(fxCache != null) {
				System.out.println("Initialized FX Deal Cache - " + fxCache.size());
			}
		} else {
			final List<FXDeal> fxDeals = new ArrayList<FXDeal>();
			final DateFormat format = new SimpleDateFormat("yyyyMMdd");
			
			for(String cptyCode : cptyCodes){
				
				if(!cptyCode.equals("\\N")){
					System.out.println("Creating fx deal filter for : " + cptyCode);
					FilterList filters = new FilterList();
					Filter f1 = new SingleColumnValueFilter(Bytes.toBytes(columnFamilyName), Bytes.toBytes("FX_counterparty_code"), CompareOp.EQUAL, Bytes.toBytes(cptyCode));
					filters.addFilter(f1);
					
					Scan scan = new Scan();
					scan.setFilter(filters);
					
					t.find("g11_fx_deal", scan, new ResultsExtractor<String>() {

						public String extractData(ResultScanner scanner) throws Exception {
							Iterator<Result> i = scanner.iterator();
							
							while(i.hasNext()){
								StringBuilder sb = new StringBuilder();
								Result r = i.next();
								sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("FX_counterparty_code")))).append(",");
								sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Deal_Status")))).append(",");
								sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Deal_type")))).append(",");
								sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Forward_point")))).append(",");
								sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Spot_Rate")))).append(",");
								sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("buy_currency_amount")))).append(",");
								sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("buy_currency_code")))).append(",");
								sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("deal_price")))).append(",");
								sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("sell_currency_code")))).append(",");
								sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("sell_currency_amount")))).append(",");
								sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("settlement_date")))).append(",");
								sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("trade_date"))));
								System.out.println(sb.toString());
								
								try{
									fxDeals.add(new FXDeal(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("FX_counterparty_code"))),
										Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("buy_currency_code"))),
										Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("sell_currency_code"))),
										Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Deal_Status"))),
										Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Deal_type"))),
										Bytes.toBigDecimal(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Forward_point"))),
										Bytes.toBigDecimal(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Spot_Rate"))),
										Bytes.toBigDecimal(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("buy_currency_amount"))),
										Bytes.toBigDecimal(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("deal_price"))),
										Bytes.toBigDecimal(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("sell_currency_amount"))),
										format.parse((Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("settlement_date"))))),
										format.parse(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("trade_date"))))));
								}catch(java.text.ParseException parseException) {
									parseException.getMessage();
								}
							}
							return null;
						}
						
					});

				}
			}

			cache.updateCache(clientId, fxDeals);
			List<FXDeal> fxCache = FXDealCache.getInstance().get(clientId);
			if(fxCache != null) {
				System.out.println("Initialized FX Deal Cache - " + fxCache.size());
			}
			System.out.println("Save cache into files");
			FileUnits.saveToDisk(dataFile, cache.getAll());
		}
		
		
	}
	
	private void scanMmDeal(String clientId, List<String> portfolioCodes){
		
		MMDealCache cache = MMDealCache.getInstance();
		String dataFile = "mmCache.dat";
		
		if(FileUnits.isFileExist(dataFile)){
			cache.initCache((HashMap <String, List <MMDeal>>)FileUnits.readFileToCache(dataFile));
			System.out.println("Read file to init cache");
			List<MMDeal> mmCache = cache.get(clientId);
			if(mmCache != null) {
				System.out.println("Initialized MM Deal Cache - " + mmCache.size());
			}
		} else {
			final List<MMDeal> mmDeals = new ArrayList<MMDeal>();
			final DateFormat format = new SimpleDateFormat("dd/MM/yyyy");
			
			for(String portfolioCode : portfolioCodes){
				if(!portfolioCode.equals("\\N")){
					System.out.println("Creating mm deal filter for : " + portfolioCode);
					List<Filter> filters = new ArrayList<Filter>();
					Filter f1 = new SingleColumnValueFilter(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Portfolio_code"), CompareOp.EQUAL, Bytes.toBytes(portfolioCode));
					filters.add(f1);
					
					FilterList filterList1 = new FilterList(filters);
					
					Scan scan = new Scan();
					scan.setFilter(filterList1);
					
					t.find("g11_mm_deal", scan, new ResultsExtractor<String>() {

						public String extractData(ResultScanner scanner) throws Exception {
							Iterator<Result> i = scanner.iterator();
							while(i.hasNext()){
								StringBuilder sb = new StringBuilder();
								Result r = i.next();
								sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Portfolio_code")))).append(",");
								sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Principal_amount")))).append(",");
								sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Trade_date")))).append(",");
								sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("currency_code")))).append(",");
								sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("interest_amount")))).append(",");
								sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("interest_rate")))).append(",");
								sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Maturity_date")))).append(",");
								System.out.println(sb.toString());
								
								try{
									mmDeals.add(new MMDeal(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Portfolio_code"))),
										Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("currency_code"))),
										Bytes.toBigDecimal(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Principal_amount"))),
										Bytes.toBigDecimal(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("interest_amount"))),
										Bytes.toBigDecimal(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("interest_rate"))),
										format.parse(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Trade_date")))),
										format.parse(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Maturity_date"))))));
								}catch(java.text.ParseException parseException) {
									System.out.println(parseException.getMessage());
								}
							}
							return null;
						}
					});
				}
			}
			
			MMDealCache.getInstance().updateCache(clientId, mmDeals);
			List<MMDeal> mmList = MMDealCache.getInstance().get(clientId);
			if(mmList != null) {
				System.out.println("Initialized FX MM List - " + mmList.size());
			}
			System.out.println("Save cache into files");
			FileUnits.saveToDisk(dataFile, cache.getAll());
		}
		
	}
	
	private void scanCashPosition(String cliengtId, List<String> cashAccountNumbers) {
		final List<CashPosition> cashPositions = new ArrayList<CashPosition>();
		final DateFormat format = new SimpleDateFormat("dd/MM/yyyy");
		
		for(String cn : cashAccountNumbers){
			if(!cn.equals("\\N")){
				System.out.println("Creating cash position filter for : " + cn);
				List<Filter> filters = new ArrayList<Filter>();
				Filter f1 = new SingleColumnValueFilter(Bytes.toBytes(columnFamilyName), Bytes.toBytes("cash_account_number"), CompareOp.EQUAL, Bytes.toBytes(cn));
				filters.add(f1);
				
				FilterList filterList1 = new FilterList(filters);
				
				Scan scan = new Scan();
				scan.setFilter(filterList1);
				
				t.find("g11_cash_position", scan, new ResultsExtractor<String>() {

					public String extractData(ResultScanner scanner) throws Exception {
						
						Iterator<Result> i = scanner.iterator();
						
						while(i.hasNext()){
							StringBuilder sb = new StringBuilder();
							Result r = i.next();
							sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("source_system_country_code")))).append(",");
							sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Source_system_code")))).append(",");
							sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("cash_account_number")))).append(",");
							sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Date")))).append(",");
							sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("currency")))).append(",");
							byte[] ledgerBalance = r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Ledger_Balance"));
							sb.append(ledgerBalance != null ? Bytes.toString(ledgerBalance) : null).append(",");
							byte[] clearedBalance = r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Cleared_balance"));
							sb.append(clearedBalance != null ? Bytes.toString(clearedBalance) : clearedBalance).append(",");
							byte[] accruedInterestAmount = r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Accrued_interest_amount"));
							sb.append(accruedInterestAmount != null ? Bytes.toString(accruedInterestAmount) : accruedInterestAmount);
							System.out.println(sb.toString());
							
							try{
								cashPositions.add(new CashPosition(Bytes.toString(r.getValue(Bytes.toBytes("cash_account_number"), Bytes.toBytes("Portfolio_code"))),
									Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Source_system_code"))),
									Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("currency"))),
									Bytes.toBigDecimal(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Cleared_balance"))),
									Bytes.toBigDecimal(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Accrued_interest_amount"))),
									format.parse(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Date"))))));
							}catch(java.text.ParseException parseException) {
								System.out.println(parseException.getMessage());
							}
						}
						return null;
					}
					
				});
			}
		}
		
		
		
		CashPositionCache.getInstance().updateCache(cliengtId, cashPositions);
		List<CashPosition> cashPositionlist = CashPositionCache.getInstance().get(cliengtId);
		if(cashPositionlist != null) {
			System.out.println("Initialized cache position list - " + cashPositionlist.size());
		}
	}
	
	private void scanCorporateEventEntitlements(String cashAccountNumber) {
		List<Filter> filters = new ArrayList<Filter>();
		Filter f1 = new SingleColumnValueFilter(Bytes.toBytes(columnFamilyName), Bytes.toBytes("cash_account_number"), CompareOp.EQUAL, Bytes.toBytes(cashAccountNumber));
		filters.add(f1);
		FilterList filterList1 = new FilterList(filters);
		
		Scan scan = new Scan();
		scan.setFilter(filterList1);
		
		final List<Entitlement> entitlements = new ArrayList<Entitlement>();
		final DateFormat format = new SimpleDateFormat("dd/MM/yyyy");
		
		t.find("g11_corporate_event_entitlements", scan, new ResultsExtractor<String>() {

			public String extractData(ResultScanner scanner) throws Exception {
				Iterator<Result> i = scanner.iterator();
				while(i.hasNext()){
					StringBuilder sb = new StringBuilder();
					Result r = i.next();
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("cash_account_number")))).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("entitlement_quantity")))).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("entitlement_rate")))).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("event_currency_code")))).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("ex_date")))).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("gross_income_amount")))).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("net_income_amount")))).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("pay_date")))).append(",");
					System.out.println(sb.toString());
					
					try{
						entitlements.add(new Entitlement(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("cash_account_number"))),
							Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("event_currency_code"))),
							Bytes.toBigDecimal(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("entitlement_quantity"))),
							Bytes.toBigDecimal(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("entitlement_rate"))),
							Bytes.toBigDecimal(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("gross_income_amount"))),
							Bytes.toBigDecimal(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("net_income_amount"))),
							format.parse(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("ex_date")))),
							format.parse(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("pay_date"))))));
					}catch(java.text.ParseException parseException) {
						System.out.println(parseException.getMessage());
					}
				}
				return null;
			}
			
		});
		
		EntitlementCache.getInstance().updateCache(cashAccountNumber, entitlements);
		List<Entitlement> cashPositionlist = EntitlementCache.getInstance().get(cashAccountNumber);
		if(cashPositionlist != null) {
			System.out.println("Initialized entitlement list - " + cashPositionlist.size());
		}
	}
	
	private void scanCustodyPosition(String cashAccountNumber) {
		
		List<Filter> filters = new ArrayList<Filter>();
		Filter f1 = new SingleColumnValueFilter(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Security_account_number"), CompareOp.EQUAL, Bytes.toBytes(cashAccountNumber));
		filters.add(f1);
		FilterList filterList1 = new FilterList(filters);
		
		Scan scan = new Scan();
		scan.setFilter(filterList1);
		
		final List<CustodyPosition> custodyPositions = new ArrayList<CustodyPosition>();
		final DateFormat format = new SimpleDateFormat("dd/MM/yyyy");
		
		t.find("g11_custody_position", scan, new ResultsExtractor<String>() {

			public String extractData(ResultScanner scanner) throws Exception {
				Iterator<Result> i = scanner.iterator();
				while(i.hasNext()){
					StringBuilder sb = new StringBuilder();
					Result r = i.next();
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Source_system_code")))).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("source_system_country_code")))).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Asset_Class")))).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Security_account_number")))).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Security_account_name")))).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Date")))).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("isin_code")))).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("sedol_code"))));
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("cusip_code"))));
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("instrument_name"))));
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("instrument_type"))));
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Location_code"))));
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("location_description"))));
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("location_country_code"))));
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Traded_Quantity"))));
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Settled_Quantity"))));
					System.out.println(sb.toString());
					
					try{
						custodyPositions.add(new CustodyPosition(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("cash_account_number"))),
							Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("instrument_name"))),
							Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("isin_code"))),
							format.parse(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Date")))),
							Bytes.toBigDecimal(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Traded_Quantity")))));
					}catch(java.text.ParseException parseException) {
						System.out.println(parseException.getMessage());
					}
				}
				return null;
			}
			
		});
		
		CustodyPositionCache.getInstance().updateCache(cashAccountNumber, custodyPositions);
		List<CustodyPosition> cashPositionlist = CustodyPositionCache.getInstance().get(cashAccountNumber);
		if(cashPositionlist != null) {
			System.out.println("Initialized custody position list - " + custodyPositions.size());
		}
	}
	
	private void scanCustodyTrade(String cashAccountNumber){
		List<Filter> filters = new ArrayList<Filter>();
		Filter f1 = new SingleColumnValueFilter(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Security_account_number"), CompareOp.EQUAL, Bytes.toBytes(cashAccountNumber));
		filters.add(f1);
		FilterList filterList1 = new FilterList(filters);
		
		Scan scan = new Scan();
		scan.setFilter(filterList1);
		
		final List<CustodyTrade> custodyTrades = new ArrayList<CustodyTrade>();
		final DateFormat format = new SimpleDateFormat("dd/MM/yyyy");
		
		t.find("g11_custody_trade", scan, new ResultsExtractor<String>() {

			public String extractData(ResultScanner scanner) throws Exception {
				Iterator<Result> i = scanner.iterator();
				while(i.hasNext()){
					StringBuilder sb = new StringBuilder();
					Result r = i.next();
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("trade_currency_code")))).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("trade_amount")))).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("trade_price")))).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("trade_status")))).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("trade_type")))).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("value_date")))).append(",");
					System.out.println(sb.toString());
					
					custodyTrades.add(new CustodyTrade(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("cash_account_number"))),
							Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("trade_status"))),
							Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("trade_type"))),
							Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("trade_currency_code"))),
							Bytes.toBigDecimal(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("trade_amount"))),
							Bytes.toBigDecimal(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("trade_price"))),
							format.parse(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("value_date"))))));
				}
				return null;
				
			}
			
		});
		
		CustodyTradeCache.getInstance().updateCache(cashAccountNumber, custodyTrades);
		List<CustodyTrade> cashTradeList = CustodyTradeCache.getInstance().get(cashAccountNumber);
		if(cashTradeList != null) {
			System.out.println("Initialized custody trade list - " + custodyTrades.size());
		}
	}
}
