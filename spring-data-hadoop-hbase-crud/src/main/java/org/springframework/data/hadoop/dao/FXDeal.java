package org.springframework.data.hadoop.dao;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

public class FXDeal implements Serializable {
	
	private String cptyCode;
	private String buyCcyCode;
	private String sellCcyCode;
	private String dealStatus;
	private String dealType;
	private BigDecimal forwadPoint;
	private BigDecimal spotRate;
	private BigDecimal buyCcyAmount;
	private BigDecimal dealPrice;
	private BigDecimal sellCcyAmount;
	private Date settlementDate;
	private Date tradeDate;

	
	
	public FXDeal(String cptyCode, String buyCcyCode, String sellCcyCode, String dealStatus, String dealType, 
			BigDecimal forwadPoint, BigDecimal spotRate, BigDecimal buyCcyAmount, BigDecimal dealPrice, 
			BigDecimal sellCcyAmount, Date settlementDate, Date tradeDate){
		
		this.cptyCode = cptyCode;
		this.buyCcyCode = buyCcyCode;
		this.sellCcyCode= sellCcyCode;
		this.dealStatus = dealStatus;
		this.dealType = dealType;
		this.forwadPoint = forwadPoint;
		this.spotRate = spotRate;
		this.buyCcyAmount = buyCcyAmount;
		this.dealPrice = dealPrice;
		this.sellCcyAmount = sellCcyAmount;
		this.settlementDate = settlementDate;
		this.tradeDate = tradeDate;
	}
	
	
	
	public String getCptyCode(){
		
		return this.cptyCode;
	}
		public String getBuyCcyCode(){
			return this.buyCcyCode;
		}
		public String getSellCcyCode(){
			
			return this.sellCcyCode;
		}
		public String getDealStatus(){
			
			return this.dealStatus;
		}
		public String getDealType(){
			return this.dealType;
		}
		public BigDecimal getForwadPoint(){
			return this.forwadPoint;
		}
		public BigDecimal getSpotRate(){
			return this.spotRate;
		}
			
		public BigDecimal getBuyCcyAmount(){
			return this.buyCcyAmount;
		}
		public BigDecimal getDealPrice(){
			return this.dealPrice;
		}
		public BigDecimal getSellCcyAmount(){
			return this.sellCcyAmount;
		}
		public Date getSettlementDate(){
			return this.settlementDate;
		}
		public Date getTradeDate(){
			return this.tradeDate;
		}

}
