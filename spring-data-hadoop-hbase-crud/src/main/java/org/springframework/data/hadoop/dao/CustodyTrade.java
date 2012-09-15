package org.springframework.data.hadoop.dao;

import java.math.BigDecimal;

public class CustodyTrade {
	
	private String cashAcNum;
	
	private String tradeStatus;
	
	private String tradeType;
	
	private String tradeCcyCode;
	
	private BigDecimal tradeAmount;
	
	private BigDecimal tradePrice;

	public CustodyTrade(String cashAcNum, String tradeStatus, String tradeType,
			String tradeCcyCode, BigDecimal tradeAmount, BigDecimal tradePrice) {
		super();
		this.cashAcNum = cashAcNum;
		this.tradeStatus = tradeStatus;
		this.tradeType = tradeType;
		this.tradeCcyCode = tradeCcyCode;
		this.tradeAmount = tradeAmount;
		this.tradePrice = tradePrice;
	}

	public String getCashAcNum() {
		return cashAcNum;
	}

	public String getTradeStatus() {
		return tradeStatus;
	}

	public String getTradeType() {
		return tradeType;
	}

	public String getTradeCcyCode() {
		return tradeCcyCode;
	}

	public BigDecimal getTradeAmount() {
		return tradeAmount;
	}

	public BigDecimal getTradePrice() {
		return tradePrice;
	}
	
}
