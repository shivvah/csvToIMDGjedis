package com.globalpay;

import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;

public class AddMapEntriesThread implements Runnable {

	boolean printsysout = false;

	private RandomValues randomValues;
	private Map<String, String> PANSEQMap;
	private Map<String, Token2> SEQGTMTMap;
	private Map<String, String> PANGTMap;
	private static Jedis jedis;
	//private static ShardedJedis jedis;

	

	
	public AddMapEntriesThread(RandomValues rv,
			Map<String, String> f1,
			Map<String, Token2> f2, 
			Map<String, String> pangt,
			Jedis jedis) 
	{
		this.randomValues = rv;
		this.PANSEQMap = f1;
		this.SEQGTMTMap = f2;
		this.PANGTMap = pangt;
		this.jedis=jedis;
	}

	public void run() 
	{

		if ( null == PANSEQMap || null == SEQGTMTMap || null == PANGTMap || null == randomValues ) System.out.println("AddMapEntriesThread.run: passed value is null.");
		
		try 
		{
			
			jedis.set(this.randomValues.getPAN(), this.randomValues.getGlobalToken());
			System.out.println("kaka jedis has been set");
			 /*PANSEQMap.put(this.randomValues.getPAN(), this.randomValues.getSequenceNumber());
			 SEQGTMTMap.put(this.randomValues.getSequenceNumber(), new Token2(this.randomValues.getGlobalToken(), this.randomValues.getMerchantToken()));
			 PANGTMap.put(this.randomValues.getPAN(), this.randomValues.getGlobalToken());*/
			
		}
		catch(Exception e)
		{
			
		}
		
	}

}
