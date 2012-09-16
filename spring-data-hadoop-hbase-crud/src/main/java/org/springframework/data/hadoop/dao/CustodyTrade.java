package org.springframework.data.hadoop.dao;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

public class CustodyTrade  implements Serializable{
	
	private String cashAcNum;
	
	private String tradeStatus;
	
	private String tradeType;
	
	private String tradeCcyCode;
	
	private BigDecimal tradeAmount;
	
	private BigDecimal tradePrice;
	
	private Date valueDate;

	public CustodyTrade(String cashAcNum, String tradeStatus, String tradeType,
			String tradeCcyCode, BigDecimal tradeAmount, BigDecimal tradePrice, Date valueDate) {
		super();
		this.cashAcNum = cashAcNum;
		this.tradeStatus = tradeStatus;
		this.tradeType = tradeType;
		this.tradeCcyCode = tradeCcyCode;
		this.tradeAmount = tradeAmount;
		this.tradePrice = tradePrice;
		this.valueDate = valueDate;
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

	public Date getValueDate() {
		return valueDate;
	}
	
	public BigDecimal getTradePrice() {
		return tradePrice;
	}
}
