package org.springframework.data.hadoop.dao;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

public class CustodyPosition  implements Serializable{
	
	private String cashAcNum;
	
	private String instrumentName;
	
	private String isinCode;
	
	private Date date;
	
	private BigDecimal tradedQuantity;

	
	public CustodyPosition (String cashAcNum, String instrumentName, String isinCode, Date date, BigDecimal tradedQuantity){
		
		this.cashAcNum = cashAcNum;
		
		this.instrumentName = instrumentName;
		
		this.isinCode = isinCode;
		
		this.date = date;
		
		this.tradedQuantity = tradedQuantity;
		
		
	}
	
	
	public String getCashAcNum(){
		
		return this.cashAcNum;
	}
	public String getInstrumentName(){
		
		return this.instrumentName;
	}
	public String getIsinCode(){
		
		return this.isinCode;
	}
	public Date getDate(){
		
		return this.date;
	}
	public BigDecimal getTradedQuantity(){
		
		return this.tradedQuantity;
	}
	
}
