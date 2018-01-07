package edu.umass.cs.capacitytesting;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.json.JSONObject;

import edu.umass.cs.capacitytesting.AbstractRequestSender;
import edu.umass.cs.capacitytesting.CapacityConfig.CapacityTestEnum;
import edu.umass.cs.gigapaxos.paxosutil.RateLimiter;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.utils.Config;


/**
 * This class creates account GUIDs used to measure the account creation rate.
 * @author ayadav
 */
public class AccountGUIDRequestSender extends AbstractRequestSender
{	
	public static enum AccountCreationMode
	{
		/**
		 * Basic account GUIDs, one account GUID per keypair.
		 */
		BASIC_ACCOUNT_GUID,
		/**
		 * Multiple account GUIDs using a single keypair.
		 */
		MULTIPLE_ACCOUNT_GUID_SINGLE_KEYPAIR,
		
		MULTIPLE_BATCH_ACCOUNT_GUID_SINGLE_KEYPAIR
	}
	
	private final Random initRand;
	
	private final String aliasPrefix;
	private final String aliasSuffix;
	private final AccountCreationMode accountCreationMode;
	private final ExecutorService threadPool;
	private final GNSClient gnsClient;
	private final long probeDuration;
	private final KeyPair keyPair;
	
	private int probeNum = 0;
	
	// Used to create the account GUID alias across multiple invocations of rateControlledRequestSender
	private long numGuidsCreatedSoFar = 0;
	
	public AccountGUIDRequestSender(long  probeDuration, String aliasPrefix, String aliasSuffix, 
			AccountCreationMode accountCreationMode, ExecutorService threadPool, GNSClient gnsClient) throws NoSuchAlgorithmException
	{
		super(Config.getGlobalDouble(CapacityTestEnum.INSERT_LOSS_TOLERANCE) );
		initRand = new Random();
		this.probeDuration = probeDuration;
		this.aliasPrefix = aliasPrefix;
		this.aliasSuffix = aliasSuffix;
		this.accountCreationMode = accountCreationMode;
		this.threadPool = threadPool;
		this.gnsClient = gnsClient;
		
		keyPair = KeyPairGenerator.getInstance(GNSProtocol.RSA_ALGORITHM.toString()).generateKeyPair();
	}
	
	public double rateControlledRequestSender(double requestRate)
	{	
		this.setStartTime();
		double totalNumUsersSent = 0;
		RateLimiter rateLimit = new RateLimiter(requestRate);
		
		long numReqs =(long) (requestRate * (probeDuration/1000));
		
		while( totalNumUsersSent < numReqs )
		{
			numSent++;
			sendAInitMessage();
			totalNumUsersSent++;
			rateLimit.record();
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("Account GUID creation. Eventual sending rate "
				+ NUMBER_FORMAT.format(sendingRate));
		
		waitForFinish();
		
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("Account GUID creation. Goodput "+ NUMBER_FORMAT.format(sysThrput));
		probeNum++;
		return sysThrput;
	}
	
	@Override
	public void incrementUpdateNumRecvd(int reqProbeNum, String userGUID, long timeTaken)
	{
		synchronized(waitLock)
		{
			numRecvd++;
			sumRequestLatency = sumRequestLatency + timeTaken;
			
			if(checkForCompletionWithLossTolerance(numSent, numRecvd))
			{
				waitLock.notify();
			}
		}
	}
	
	@Override
	public void incrementSearchNumRecvd(int reqProbeNum, int resultInfo, long timeTaken) 
	{
	}
	
	@Override
	public void incrementGetNumRecvd(int reqProbeNum, JSONObject resultJSON, long timeTaken) 
	{	
	}
	
	@Override
	public void resetCurrentRequests() 
	{
		numSent = 0;
		numRecvd = 0;
		sumRequestLatency = 0;
	}
	
	private void sendAInitMessage()
	{	
		AbstractRequestSender reqSender = this;
		threadPool.execute(
				new Runnable()
				{
					@Override
					public void run()
					{
						try
						{
							if(accountCreationMode == AccountCreationMode.MULTIPLE_ACCOUNT_GUID_SINGLE_KEYPAIR)
							{
								String alias = aliasPrefix+numGuidsCreatedSoFar+aliasSuffix;
								numGuidsCreatedSoFar++;
								gnsClient.execute(GNSCommand.createAccountUsingKeyPair(alias, null, null, keyPair, false), 
										new InitCallBack(probeNum, reqSender));
							}
							else if(accountCreationMode == AccountCreationMode.MULTIPLE_BATCH_ACCOUNT_GUID_SINGLE_KEYPAIR)
							{
								int batchSize = Config.getGlobalInt(CapacityTestEnum.BATCH_SIZE);
								Set<String> aliasSet = new HashSet<String>();
								for(int i=0; i<batchSize; i++)
								{
									String alias = aliasPrefix+numGuidsCreatedSoFar+aliasSuffix;
									numGuidsCreatedSoFar++;
									aliasSet.add(alias);
								}
								
								gnsClient.execute(GNSCommand.batchCreateAccountGUIDsUsingKeyPair(aliasSet, null, null, keyPair, false), 
										new InitCallBack(probeNum, reqSender));
								
								// long startTime = System.currentTimeMillis();
								// CommandPacket cmd = gnsClient.execute(GNSCommand.batchCreateAccountGUIDsUsingKeyPair(aliasSet, null, null, keyPair, false));
								
								
								// reqSender.incrementUpdateNumRecvd(0, cmd.getServiceName(), 
								//		(System.currentTimeMillis()-startTime));
							}
							else
							{
								String alias = aliasPrefix+numGuidsCreatedSoFar+aliasSuffix;
								numGuidsCreatedSoFar++;
								gnsClient.execute(GNSCommand.createAccount(alias), new InitCallBack(probeNum, reqSender));
							}
						} catch (ClientException | NoSuchAlgorithmException | IOException e) 
						{
							e.printStackTrace();
						}
					}
				});
	}
	
	@Override
	public double getResponseRate() 
	{
		double respRate = (numRecvd*1000.0)/(this.expFinishTime-this.expStartTime);
		return respRate;
	}
	
	@Override
	public double getAvgResponseLatency() 
	{
		return sumRequestLatency/numRecvd;
	}
}