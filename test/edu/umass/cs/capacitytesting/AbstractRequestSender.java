package edu.umass.cs.capacitytesting;

import java.text.DecimalFormat;
import java.util.Timer;
import java.util.TimerTask;

import org.json.JSONObject;

import edu.umass.cs.capacitytesting.CapacityConfig.CapacityTestEnum;
import edu.umass.cs.utils.Config;


public abstract class AbstractRequestSender
{
	public static final DecimalFormat NUMBER_FORMAT	 			= new DecimalFormat("#.###");
	
	protected long expStartTime;
	protected long expFinishTime;
	protected Timer waitTimer;
	protected final Object waitLock = new Object();
	protected long numSent;
	protected long numRecvd;
	protected long sumRequestLatency;
	
	private final double lossTolerance;
	
	public AbstractRequestSender( double lossTolerance )
	{
		//threadFinished = false;
		this.lossTolerance = lossTolerance;
		numSent = 0;
		numRecvd = 0;
		sumRequestLatency = 0;
	}
	
	protected void setStartTime()
	{
		expStartTime = System.currentTimeMillis();
	}
	
	protected void setFinishTime()
	{
		expFinishTime = System.currentTimeMillis(); 
	}
	
	protected void waitForFinish()
	{
		waitTimer = new Timer();
		waitTimer.schedule(new WaitTimerTask(), Config.getGlobalInt(CapacityTestEnum.WAIT_TIME));
		
		synchronized(waitLock)
		{
			while( !checkForCompletionWithLossTolerance(numSent, numRecvd) )
			{
				try
				{
					waitLock.wait();
				} catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			}
		}
		this.setFinishTime();
		waitTimer.cancel();
	}
	
	public class WaitTimerTask extends TimerTask
	{			
		@Override
		public void run()
		{			
			setFinishTime();
			double sysThrput= (numRecvd * 1000.0)/(expFinishTime - expStartTime);
			
			System.out.println(this.getClass().getName()
						+" Result:TimeOutThroughput "+sysThrput);
			
			waitTimer.cancel();
		}
	}
	
	protected boolean checkForCompletionWithLossTolerance
											(double numSent, double numRecvd)
	{
		boolean completion = false;
		
		double withinLoss = (lossTolerance * numSent)/100.0;
		if( (numSent - numRecvd) <= withinLoss )
		{
			completion = true;
		}
		return completion;
	}
	
	/**
	 * Resets the counters and the current requests.
	 */
	public abstract void resetCurrentRequests();
	
	/**
	 * Sends requests at the specified rate.
	 * @param requestRate
	 * @return returns the system throughput at the specified requestRate
	 */
	public abstract double rateControlledRequestSender(double requestRate);
	
	public abstract void incrementUpdateNumRecvd(int reqProbeNum, String userGUID, long timeTaken);
	public abstract void incrementSearchNumRecvd(int reqProbeNum, int resultSize, long timeTaken);
	public abstract void incrementGetNumRecvd(int reqProbeNum, JSONObject resultJSON, long timeTaken);
	
	public abstract double getResponseRate();
	public abstract double getAvgResponseLatency();
}