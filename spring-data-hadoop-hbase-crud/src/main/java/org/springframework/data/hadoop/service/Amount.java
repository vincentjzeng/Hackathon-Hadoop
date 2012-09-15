package org.springframework.data.hadoop.service;

import java.math.BigDecimal;

public class Amount {
	
	private BigDecimal currentAmount;
	
	private BigDecimal forcastAmount;
	
	public BigDecimal getCurrentAllAmount(){
		
		return null;
		
	}
	
	public BigDecimal getFocastingAllAmount(){
		
		return null;
	}
	
	
	public void setCurrentAllAmount(BigDecimal currentAmount){
		
		this.currentAmount = currentAmount;
	}
	
	public  void setFocastingAllAmount(BigDecimal forcastAmount){
		
		this.forcastAmount = forcastAmount;
	}


}
