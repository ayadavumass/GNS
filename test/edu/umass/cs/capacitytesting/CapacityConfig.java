/* Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): V. Arun */
package edu.umass.cs.capacitytesting;

import java.io.IOException;

import edu.umass.cs.utils.Config;

/**
 * @author ayadav
 * 
 *         Configuration parameters for the capacity tests in the GNS.
 */
public class CapacityConfig 
{	
	/**
	 * 
	 */
	public static final String TESTING_CONFIG_FILE_KEY = "capacityTestConfig";
	/**
	 * 
	 */
	public static final String DEFAULT_TESTING_CONFIG_FILE = "conf/capacityTest.properties";

	public static void load() 
	{
		// testing specific config parameters
		try 
		{
			Config.register(CapacityTestEnum.class, TESTING_CONFIG_FILE_KEY,
					DEFAULT_TESTING_CONFIG_FILE);
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Capacity test  config parameters.
	 */
	public static enum CapacityTestEnum implements Config.ConfigurableEnum {

		/**
		 * Insert loss tolerance in perencetage of requests.
		 */
		INSERT_LOSS_TOLERANCE(0.0),
		
		
		/**
		 * Fraction of load above which the response rate must be for the
		 * capacity probe to be considered successful. This does not mean
		 * anything except that the run will be marked "FAILED".
		 */
		PROBE_RESPONSE_THRESHOLD(0.9),
		
		/**
		 * Factor by which capacity probe load will be increased in each step.
		 */
		PROBE_LOAD_INCREASE_FACTOR(1.1),
		
		/**
		 * Strting load for the probe
		 */
		PROBE_INIT_LOAD(1),
		
		/**
		 * Threshold on average response time for a probe run to be considered
		 * successful.
		 */
		PROBE_LATENCY_THRESHOLD(8000),
		
		/**
		 * Maximum number of consecutive failures afte which a capacity probe
		 * will be given up.
		 */
		PROBE_MAX_CONSECUTIVE_FAILURES(8),
		
		
		/**
		 * Stop after these many probe runs.
		 */
		PROBE_MAX_RUNS(Integer.MAX_VALUE),
		
		
		/**
		 * Response wait time in ms.
		 */
		WAIT_TIME(30000),  
		
		/**
		 * Alias prefix
		 */
		ALIAS_PREFIX("UserGUID"),
		
		/**
		 * 
		 */
		ALIAS_SUFFIX("@gmail.com"),
		
		/**
		 * Duration of each capacity probe in ms.
		 */
		PROBE_RUN_DURATION(30000),
		
		/**
		 * If true then internal probe prints are printed on System.out
		 */
		PROBE_PRINTS(true),
		
		
		/**
		 * Request sending thread pool size
		 */
		THREAD_POOL_SIZE(10),
		
		/**
		 * Timeout for GNS requests in ms
		 */
		GNS_REQ_TIMEOUT(8000),
		
		/**
		 * Number of retries for failed request in GNS client
		 */
		GNS_NUM_RETRIES(5),
		
		/**
		 * Used to set the maximum outstanding app requests
		 * in the {@link GNSClient#setMaximumOutstandingAppRequests}
		 */
		MAX_OUTSTANDING_APP_REQUESTS(100000),
		;
		
		
		final Object defaultValue;

		CapacityTestEnum(Object defaultValue) {
			this.defaultValue = defaultValue;
		}

		@Override
		public Object getDefaultValue() {
			return this.defaultValue;
		}
		
		@Override
		public String getDefaultConfigFile() {
			return DEFAULT_TESTING_CONFIG_FILE;
		}
		
		@Override
		public String getConfigFileKey() {
			return TESTING_CONFIG_FILE_KEY;
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) 
	{
		CapacityConfig.load();
		System.out.println("Thread pool size "+Config.getGlobalInt(CapacityTestEnum.THREAD_POOL_SIZE));
	}
}