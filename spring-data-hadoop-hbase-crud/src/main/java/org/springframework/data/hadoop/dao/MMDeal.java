package org.springframework.data.hadoop.dao;

import java.math.BigDecimal;
import java.util.Date;

public class MMDeal {
	
	private String portfolioCode;
	private String ccyCode;
	private BigDecimal principalAmount;
	private BigDecimal interestAmount;
	private BigDecimal interestRate;
	private Date tradeDate;
	private Date maturityDate;
	

	public MMDeal(String portfolioCode, String ccyCode, BigDecimal principalAmount, BigDecimal interestAmount,
			BigDecimal interestRate, Date tradeDate, Date maturityDate){
		
		this.portfolioCode = portfolioCode;
		this.ccyCode = ccyCode;
		this.principalAmount = principalAmount;
		this.interestAmount = interestAmount;
		this.interestRate = interestRate;
		this.tradeDate = tradeDate;
		this.maturityDate = maturityDate;
		
	}
	
	public String getPortfolioCode(){
		
		return this.portfolioCode;
	}
	public String getCcyCode(){
		return this.ccyCode;
	}
	public BigDecimal getPrincipalAmount(){
		return this.principalAmount;
	}
	public BigDecimal getInterestAmount(){
		return this.interestAmount;
	}
	public BigDecimal getInterestRate(){
		return this.interestRate;
	}
	public Date getTradeDate(){
		return this.tradeDate;
	}
	public Date getMaturityDate(){
		return this.maturityDate;
	}
	
	
}
