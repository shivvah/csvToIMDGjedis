package com.globalpay;

public class RandomValues 
{
	private String PAN;
	private String SequenceNumber;
	private String GlobalToken;
	private String MerchantToken;
	
	public RandomValues()
	{
		
	}
	public RandomValues(String pan, String sequenceNumber, String globalToken, String merchantToken)
	{
		this.PAN = pan;
		this.SequenceNumber = sequenceNumber; 
		this.GlobalToken = globalToken;
		this.MerchantToken = merchantToken;
	}
	public String getPAN() {
		return PAN;
	}
	public void setPAN(String pAN) {
		PAN = pAN;
	}
	public String getSequenceNumber() {
		return SequenceNumber;
	}
	public void setSequenceNumber(String sequenceNumber) {
		SequenceNumber = sequenceNumber;
	}
	public String getGlobalToken() {
		return GlobalToken;
	}
	public void setGlobalToken(String globalToken) {
		GlobalToken = globalToken;
	}
	public String getMerchantToken() {
		return MerchantToken;
	}
	public void setMerchantToken(String merchantToken) {
		MerchantToken = merchantToken;
	}
	
}
