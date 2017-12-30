package edu.umass.cs.gnsclient.client.singletests;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import edu.umass.cs.capacitytesting.CapacityConfig;
import edu.umass.cs.capacitytesting.CapacityConfig.CapacityTestEnum;
import edu.umass.cs.capacitytesting.AbstractRequestSender;
import edu.umass.cs.capacitytesting.AccountGUIDRequestSender;
import edu.umass.cs.capacitytesting.AccountGUIDRequestSender.AccountCreationMode;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)

public class AccountCreationCapacityTest extends DefaultGNSTest
{
	static 
	{
		CapacityConfig.load();
	}
	
	/** 
	 * Initial probe starting load.
	 */
	public static double PROBE_INIT_LOAD						= Config.getGlobalDouble(CapacityTestEnum.PROBE_INIT_LOAD);
	
	/**
	 * Fraction of load above which the response rate must be for the
	 * capacity probe to be considered successful. This does not mean
	 * anything except that the run will be marked "FAILED".
	 */
	public static final double PROBE_RESPONSE_THRESHOLD			= Config.getGlobalDouble(CapacityTestEnum.PROBE_RESPONSE_THRESHOLD);
	
	/**
	 * Threshold on average response time for a probe run to be considered
	 * successful.
	 */
	public static double PROBE_LATENCY_THRESHOLD				= Config.getGlobalDouble(CapacityTestEnum.PROBE_LATENCY_THRESHOLD);
	
	/**
	 * Maximum number of consecutive failures afte which a capacity probe
	 * will be given up.
	 */
	public static final int PROBE_MAX_CONSECUTIVE_FAILURES		= Config.getGlobalInt(CapacityTestEnum.PROBE_MAX_CONSECUTIVE_FAILURES);
	
	/**
	 * Stop after these many probe runs.
	 */
	public static final int PROBE_MAX_RUNS						= Config.getGlobalInt(CapacityTestEnum.PROBE_MAX_RUNS);
	
	/**
	 * Factor by which capacity probe load will be increased in each step.
	 */
	public static final double PROBE_LOAD_INCREASE_FACTOR		= Config.getGlobalDouble(CapacityTestEnum.PROBE_LOAD_INCREASE_FACTOR);
	
	
	public static final boolean PROBE_PRINTS					= Config.getGlobalBoolean(CapacityTestEnum.PROBE_PRINTS);
	
	public static final long PROBE_RUN_DURATION					= Config.getGlobalLong(CapacityTestEnum.PROBE_RUN_DURATION);
	
	public static String ALIAS_PREFIX							= "TestGUID";
	public static final String ALIAS_SUFFIX						= "@gns.name";
	
	public static final int THREAD_POOL_SIZE 					= Config.getGlobalInt(CapacityTestEnum.THREAD_POOL_SIZE);
	
	private static final ExecutorService THREAD_POOL = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
	
	public AccountCreationCapacityTest()
	{
	}
	
	@Test
	public void test_1_basicAccountCreation() 
	{
		// This test doesn't clean all the created guids. So, it assumes that each time test is started by resetting the GNS.
		AccountGUIDRequestSender accountGuidRequestSender = 
				new AccountGUIDRequestSender(PROBE_RUN_DURATION, ALIAS_PREFIX, ALIAS_SUFFIX, 
						AccountCreationMode.BASIC_ACCOUNT_GUID, THREAD_POOL, client);
		
		double capacity = probeCapacity(accountGuidRequestSender);
		System.out.println("Basic account creation capacity is "+capacity+" account GUIDs/sec");
	}
	
	@Test
	public void test_2_accountCreationWithSingleKeypair()
	{	
	}
	
	
	/**
	 * This method probes for the capacity by multiplicatively increasing the
	 * load until the response rate is at least a threshold fraction of the
	 * injected load and the average response time is within a threshold. 
	 * 
	 * @param requestSender
	 * @return capacity
	 */
	private double probeCapacity(AbstractRequestSender requestSender) 
	{
		
		double load = PROBE_INIT_LOAD;
		double responseRate = 0, capacity = 0, latency = Double.MAX_VALUE;
		double threshold = PROBE_RESPONSE_THRESHOLD, 
				loadIncreaseFactor = PROBE_LOAD_INCREASE_FACTOR, 
				minLoadIncreaseFactor = 1.01;
		
		int runs = 0, consecutiveFailures = 0;

		/**************** Start of capacity probes *******************/
		do {
			if (runs++ > 0)
				// increase probe load only if successful
				if (consecutiveFailures == 0)
					load *= loadIncreaseFactor;
				else
					// scale back if failed
					load *= (1 - (loadIncreaseFactor - 1) / 2);

			/* Two failures => increase more cautiously. Sometimes a failure
			 * happens in the very first run if the JVM is too cold, so we wait
			 * for at least two consecutive failures. */
			if (consecutiveFailures == 2)
				loadIncreaseFactor = (1 + (loadIncreaseFactor - 1) / 2);
			
			// we are within roughly 0.1% of capacity
			if (loadIncreaseFactor < minLoadIncreaseFactor)
				break;

			/* Need to clear requests from previous run, otherwise the response
			 * rate can be higher than the sent rate, which doesn't make sense. */
			
			requestSender.resetCurrentRequests();
			
			if(PROBE_PRINTS)
				System.out.println("Testing at load "+load);
			
			requestSender.rateControlledRequestSender(load);

			responseRate = requestSender.getResponseRate();

			latency = requestSender.getAvgResponseLatency();
			
			if (latency < PROBE_LATENCY_THRESHOLD)
				capacity = Math.max(capacity, responseRate);
			
			boolean success = (responseRate > threshold * load 
							&& latency <= PROBE_LATENCY_THRESHOLD);
			
			System.out.println("capacity >= " + Util.df(capacity)
					+ "/s "+(!success ? " and capacity <= "+ Util.df(Math.max(capacity, load))+";": ";")
					+ " (response_rate=" + Util.df(responseRate)
					+ "/s, average_response_time=" + Util.df(latency) + "ms)");
			try 
			{
				Thread.sleep(2000);
			} 
			catch (InterruptedException e) 
			{
				e.printStackTrace();
			}
			
			if (success)
				consecutiveFailures = 0;
			else
				consecutiveFailures++;
		} while (consecutiveFailures < PROBE_MAX_CONSECUTIVE_FAILURES
				&& runs < PROBE_MAX_RUNS);
		/**************** End of capacity probes *******************/
		System.out.println("capacity <= "
						+ Util.df(Math.max(capacity, load))
						+ ", stopping probes because"
						+ (capacity < threshold * load ? " response_rate was less than 95% of injected load"
								+ Util.df(load) + "/s; "
								: "")
						+ (latency > PROBE_LATENCY_THRESHOLD ? " average_response_time="
								+ Util.df(latency)
								+ "ms"
								+ " >= "
								+ PROBE_LATENCY_THRESHOLD
								+ "ms;"
								: "")
						+ (loadIncreaseFactor < minLoadIncreaseFactor ? " capacity is within "
								+ Util.df((minLoadIncreaseFactor - 1) * 100)
								+ "% of next probe load level;"
								: "")
						+ (consecutiveFailures > PROBE_MAX_CONSECUTIVE_FAILURES ? " too many consecutive failures;"
								: "")
						+ (runs >= PROBE_MAX_RUNS ? " reached limit of "
								+ PROBE_MAX_RUNS
								+ " runs;"
								: ""));
		return capacity;
	}
}