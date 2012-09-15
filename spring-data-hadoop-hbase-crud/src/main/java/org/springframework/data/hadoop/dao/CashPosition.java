package org.springframework.data.hadoop.dao;

import java.math.BigDecimal;
import java.util.Date;

public class CashPosition {
	
	private String cashAcNum;
	private String scrSystemCode;
	private String ccy;
	private BigDecimal clearedBalance;
	private BigDecimal accruedInterestAmount;
	private Date date;
	
	public CashPosition (String cashAcNum, String scrSystemCode, String ccy, BigDecimal clearedBalance,
			BigDecimal accruedInterestAmount, Date date){
		
		this.cashAcNum = cashAcNum;
		this.scrSystemCode = scrSystemCode;
		this.ccy = ccy;
		this.clearedBalance = clearedBalance;
		this.accruedInterestAmount = accruedInterestAmount;
		this.date = date;
		
	}
	

	public String getCashAcNum(){
		
		return this.cashAcNum;
	}
	public String getScrSystemCode(){
		return this.scrSystemCode;
	}
	public String getCcy(){
		return this.ccy;
	}
	public BigDecimal getClearedBalance(){
		
		return this.clearedBalance;
	}
	public BigDecimal getAccruedInterestAmount(){
		return this.accruedInterestAmount;
	}
	public Date getDate(){
		return this.date;
	}

	
}
