package org.springframework.data.hadoop.dao;

import java.math.BigDecimal;
import java.util.Date;

public class Entitlement {
	
	private String cashAcNum;
	private String eventCcyCode;
	private BigDecimal entitlementQuantity;
	private BigDecimal entitlementRate;
	private BigDecimal grossIncomeAmount;
	private BigDecimal netIncomeAmount;
	private Date exDate;
	private Date payDate;

	
	public Entitlement (String cashAcNum, String eventCcyCode, BigDecimal entitlementQuantity,
			BigDecimal entitlementRate, BigDecimal grossIncomeAmount , BigDecimal netIncomeAmount,
			Date exDate, Date payDate){
		
		this.cashAcNum = cashAcNum;
		this.eventCcyCode = eventCcyCode;
		this.entitlementQuantity = entitlementQuantity;
		this.entitlementRate = entitlementRate;
		this.grossIncomeAmount = grossIncomeAmount;
		this.netIncomeAmount = netIncomeAmount;
		this.exDate = exDate;
		this.payDate = payDate;
	}
	
	public String getCashAcNum(){
		return this.cashAcNum;
	}
	public String getEventCcyCode(){
		return this.eventCcyCode;
		
	}
	public BigDecimal getEntitlementQuantity(){
		return this.entitlementQuantity;
	}
	public BigDecimal getEntitlementRate(){
		
		return this.entitlementRate;
	}
	public BigDecimal getGrossIncomeAmount(){
		return this.grossIncomeAmount;
	}
	public BigDecimal getNetIncomeAmount(){
		return this.netIncomeAmount;
	}
	public Date getExDate(){
		return this.exDate;
	}
	public Date getPayDate(){
		return this.payDate;
	}
}
