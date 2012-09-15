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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

	String rowName = "row";
	String client_id = "client_id";
	String columnFamilyName = "cf";
	String tableName = "g11_client_hierarchy";
	String cellValue = "http://blog.springsource.org/2012/02/29/introducing-spring-hadoop/";

	/**
	 * Main entry point.
	 */
	@PostConstruct
	public void run() throws Exception {
		System.out.println("Application starting...");

		//4. get 1 column data
		//readData();

		//5. scan data
		//scanClientHierarchy();
		//scanCashPosition();
		scanPosition();

		System.out.println("Application started");
	}

	/**
	 * Read data from table.
	 * 
	 */
	private void readData() {
		// replace with get 
		System.out.println(t.execute(tableName, new TableCallback<Long>() {
			public Long doInTable(HTable table) throws Throwable {
				Get get = new Get(Bytes.toBytes(rowName + "2"));
				Result result = table.get(get);
				byte[] valueByte = result.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes(client_id));
				return Bytes.toLong(valueByte);
			}
		}));
	}

	/**
	 * Scan table data.
	 * 
	 */

	private void scanClientHierarchy() {
		/*
		t.find(tableName, columnFamilyName, client_id, new RowMapper<String>() {
			public String mapRow(Result result, int rowNum) throws Exception {
				System.out.println("Row key :" + result.getRow());
				System.out.println("Key value :" + result.getRow());
				return Bytes.toString(result.value());
			}
		});
		*/
		
		/*
		t.find(tableName, columnFamilyName, new ResultsExtractor<String>() {

			public String extractData(ResultScanner scanner) throws Exception {
				Iterator<Result> i = scanner.iterator();
				while(i.hasNext()){
					System.out.println("Client ID: " + Bytes.toString(i.next().getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes(client_id))));
					System.out.println("FXMM code: " + Bytes.toString(i.next().getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("fxmm_code"))));
					System.out.println("Security Account Number: " + Bytes.toString(i.next().getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("security_account_number"))));
					System.out.println("Source System Country code: " + Bytes.toString(i.next().getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("source_system_country_code"))));
					System.out.println("Source System Code: " + Bytes.toString(i.next().getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Source_system_code"))));
					System.out.println("Calypso Counterparty ID: " + Bytes.toString(i.next().getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("calypso_counterpty_id"))));
					System.out.println("Cash Account Number: " + Bytes.toString(i.next().getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("cash_account_number"))));
				}
				return null;
			}
			
		});
		*/
		
		List<Filter> filters = new ArrayList<Filter>();
		Filter clientIdFilter = new SingleColumnValueFilter(Bytes.toBytes(columnFamilyName), Bytes.toBytes("client_id"), CompareOp.EQUAL, Bytes.toBytes("11"));
		filters.add(clientIdFilter);
		FilterList filterList1 = new FilterList(filters);
		
		Scan scan = new Scan();
		scan.setFilter(filterList1);
		
		t.find(tableName, scan, new ResultsExtractor<String>() {

			public String extractData(ResultScanner scanner) throws Exception {
				Iterator<Result> i = scanner.iterator();
				System.out.println("Client ID, FXMM code, Security Account Number, Source System Country code, Source System Code, Calypso Counterparty ID, Cash Account Number\n");
				while(i.hasNext()){
					StringBuilder sb = new StringBuilder();
					Result r = i.next();
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes(client_id)))).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("fxmm_code")))).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("security_account_number")))).append(",");
					byte[] sourceSystemCountryCode = r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("source_system_country_code"));
					sb.append(sourceSystemCountryCode != null ? Bytes.toString(sourceSystemCountryCode) : null).append(",");
					byte[] sourceSystemCode = r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("Source_system_code"));
					sb.append(sourceSystemCode != null ? Bytes.toString(sourceSystemCode) : sourceSystemCode).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("calypso_counterpty_id")))).append(",");
					sb.append(Bytes.toString(r.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("cash_account_number"))));
					System.out.println(sb.toString());
				}
				return null;
			}
			
		});
	}
	
	private void scanCashPosition() {
		
		List<Filter> filters = new ArrayList<Filter>();
		Filter f1 = new SingleColumnValueFilter(Bytes.toBytes(columnFamilyName), Bytes.toBytes("source_system_country_code"), CompareOp.EQUAL, Bytes.toBytes("UK"));
		filters.add(f1);
		FilterList filterList1 = new FilterList(filters);
		
		Scan scan = new Scan();
		scan.setFilter(filterList1);
		
		t.find("g11_cash_position", scan, new ResultsExtractor<String>() {

			public String extractData(ResultScanner scanner) throws Exception {
				Iterator<Result> i = scanner.iterator();
				System.out.println("Source System Country Code,  Source System Coce, Cash Account Number, Date, Currency, Ledger Balance, Cleard Balance, Accrued Interest Amount\n");
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
				}
				return null;
			}
			
		});
	}
	
	private void scanPosition() {
		
		List<Filter> filters = new ArrayList<Filter>();
		Filter f1 = new SingleColumnValueFilter(Bytes.toBytes(columnFamilyName), Bytes.toBytes("source_system_country_code"), CompareOp.EQUAL, Bytes.toBytes("UK"));
		filters.add(f1);
		FilterList filterList1 = new FilterList(filters);
		
		Scan scan = new Scan();
		scan.setFilter(filterList1);
		
		t.find("g11_custody_position", scan, new ResultsExtractor<String>() {

			public String extractData(ResultScanner scanner) throws Exception {
				Iterator<Result> i = scanner.iterator();
				System.out.println("Source_system_code,  source_system_country_code, Asset_Class, Security_account_number, Security_account_name, Date," +
						"isin_code, sedol_code, cusip_code, instrument_name, instrument_type, Location_code, location_description, location_country_code," +
						"Traded_Quantity, Settled_Quantity\n");
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
				}
				return null;
			}
			
		});
	}
}