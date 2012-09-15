package org.springframework.data.hadoop.dao;

import java.math.BigDecimal;
import java.util.Date;

public class CustodyPosition {
	
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
	
	
	public String cashAcNum(){
		
		return this.cashAcNum;
	}
	public String instrumentName(){
		
		return this.instrumentName;
	}
	public String isinCode(){
		
		return this.isinCode;
	}
	public Date date(){
		
		return this.date;
	}
	public BigDecimal tradedQuantity(){
		
		return this.tradedQuantity;
	}
	
}
