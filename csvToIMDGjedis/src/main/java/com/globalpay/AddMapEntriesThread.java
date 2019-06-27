package com.globalpay;

import java.util.Map;

import redis.clients.jedis.Jedis;

public class AddMapEntriesThread implements Runnable {

	boolean printsysout = false;

	private RandomValues randomValues;
	private Map<String, String> PANSEQMap;
	private Map<String, Token2> SEQGTMTMap;
	private Map<String, String> PANGTMap;

	

	
	public AddMapEntriesThread(RandomValues rv,
			Map<String, String> f1,
			Map<String, Token2> f2, 
			Map<String, String> pangt) 
	{
		this.randomValues = rv;
		this.PANSEQMap = f1;
		this.SEQGTMTMap = f2;
		this.PANGTMap = pangt;
		
	}

	public void run() 
	{

		if ( null == PANSEQMap || null == SEQGTMTMap || null == PANGTMap || null == randomValues ) System.out.println("AddMapEntriesThread.run: passed value is null.");
		
		try 
		{
			 PANSEQMap.put(this.randomValues.getPAN(), this.randomValues.getSequenceNumber());
			 SEQGTMTMap.put(this.randomValues.getSequenceNumber(), new Token2(this.randomValues.getGlobalToken(), this.randomValues.getMerchantToken()));
			 PANGTMap.put(this.randomValues.getPAN(), this.randomValues.getGlobalToken());
			
		}
		catch(Exception e)
		{
			
		}
		
	}

}
