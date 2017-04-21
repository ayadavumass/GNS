package edu.umass.cs.gnsserver.gnsapp.demandprofiles;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.VotesMap;
import edu.umass.cs.gnsserver.utils.Util;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.InterfaceGetActiveIPs;


/**
 * The description for the demand profile implemented in this class is as follows:
 * 1) We partition the set of n nodes into sqrt(n) MEE sets of sqrt(n) size each.
 * 2) For a GUID, there is one active replica in each of the sqrt(n) partitions computed in
 *    step 1. We decide an active replica for a GUID in a partition of sqrt(n) nodes by hashing that 
 *    GUID to one node among sqrt(n) nodes in the partition. We perform the same procedure to decide
 *    active replicas  in all partitions. 
 * 3) For a simple implementation, the computation of active replicas for a GUID happens only once 
 *    in the lifetime of the system.
 * 4) Currently, this approach doesn't handle if the set of total nodes change.
 * @author ayadav
 */
public class SqrtNReplicationDemandProfile extends AbstractDemandProfile 
{
	private static final Logger LOG = Logger.getLogger(SqrtNReplicationDemandProfile.class.getName());
	
	/**
	 * reconfigurationHappened keeps track if the reconfiguration for a service name, GUID in GNS,
	 * corresponding to this demand profile has happened once or not.
	 */
	private boolean reconfigurationHappened = false;
	
	
	/**
	 * The keys for the demand profile packet.
	 * Most of these fields are not used in this demand profile, but we keep them for 
	 * priting stats.
	 */
	private enum Keys {
		/**
		 * SERVICE_NAME
		 */
		SERVICE_NAME,
	    /**
	     * STATS
	     */
		STATS,
	    /**
	     * RATE
	     */
		RATE,
	    /**
	     * NUM_REQUESTS
	     */
		NUM_REQUESTS,
	    /**
	     * NUM_TOTAL_REQUESTS
	     */
		NUM_TOTAL_REQUESTS,
	    /**
	     * VOTES_MAP
	     */
		VOTES_MAP,
	    /**
	     * LOOKUP_COUNT
	     */
		LOOKUP_COUNT,
	    /**
	     * UPDATE_COUNT
	     */
		UPDATE_COUNT
	  };
	  
	  private double interArrivalTime = 0.0;
	  private long lastRequestTime = 0;
	  private int numRequests = 0;
	  private int numTotalRequests = 0;
	  private SqrtNReplicationDemandProfile lastReconfiguredProfile = null;
	  private VotesMap votesMap = new VotesMap();
	  private int lookupCount = 0;
	  private int updateCount = 0;
	  
	  
	  public SqrtNReplicationDemandProfile(String name) 
	  {
		  super(name);
	  }
	  
	  /**
	   * Create a SqrtNReplicationDemandProfile instance by making 
	   * a deep copy of another instance.
	   *
	   * @param dp
	   */
	  public SqrtNReplicationDemandProfile(SqrtNReplicationDemandProfile dp) {
	    super(dp.name);
	    this.interArrivalTime = dp.interArrivalTime;
	    this.lastRequestTime = dp.lastRequestTime;
	    this.numRequests = dp.numRequests;
	    this.numTotalRequests = dp.numTotalRequests;
	    this.votesMap = new VotesMap(dp.votesMap);
	    this.lookupCount = dp.lookupCount;
	    this.updateCount = dp.updateCount;
	  }
	  
	  /**
	   * Create a SqrtNReplicationDemandProfile instance from a JSON packet.
	   *
	   * @param json
	   * @throws org.json.JSONException
	   */
	  public SqrtNReplicationDemandProfile(JSONObject json) throws JSONException 
	  {
	    super(json.getString(Keys.SERVICE_NAME.toString()));
	    this.interArrivalTime = 1.0 / json.getDouble(Keys.RATE.toString());
	    this.numRequests = json.getInt(Keys.NUM_REQUESTS.toString());
	    this.numTotalRequests = json.getInt(Keys.NUM_TOTAL_REQUESTS.toString());
	    this.votesMap = new VotesMap(json.getJSONObject(Keys.VOTES_MAP.toString()));
	    this.lookupCount = json.getInt(Keys.LOOKUP_COUNT.toString());
	    this.updateCount = json.getInt(Keys.UPDATE_COUNT.toString());
	    LOG.log(Level.FINE, "%%%%%%%%%%%%%%%%%%%%%%%%%>>> {0} VOTES MAP AFTER READ: {1}", new Object[]{this.name, this.votesMap});
	  }
	  
	  /**
	   *
	   * @return the stats
	   */
	  @Override
	  public JSONObject getStats() {
	    LOG.log(Level.FINE, "%%%%%%%%%%%%%%%%%%%%%%%%%>>> {0} VOTES MAP BEFORE GET STATS: {1}", new Object[]{this.name, this.votesMap});
	    JSONObject json = new JSONObject();
	    try {
	      json.put(Keys.SERVICE_NAME.toString(), this.name);
	      json.put(Keys.RATE.toString(), getRequestRate());
	      json.put(Keys.NUM_REQUESTS.toString(), getNumRequests());
	      json.put(Keys.NUM_TOTAL_REQUESTS.toString(), getNumTotalRequests());
	      json.put(Keys.VOTES_MAP.toString(), getVotesMap().toJSONObject());
	      json.put(Keys.LOOKUP_COUNT.toString(), this.lookupCount);
	      json.put(Keys.UPDATE_COUNT.toString(), this.updateCount);
	    } catch (JSONException je) {
	      je.printStackTrace();
	    }
	    LOG.log(Level.FINE, "%%%%%%%%%%%%%%%%%%%%%%%%%>>> {0} GET STATS: {1}", 
	    											new Object[]{this.name, json});
	    return json;
	  }
	  
	  /**
	   * Create an empty SqrtNReplicationDemandProfile instance for a name.
	   *
	   * @param name
	   * @return New demand profile for {@code name}.
	   */
	  public static SqrtNReplicationDemandProfile createDemandProfile(String name) 
	  {
		  return new SqrtNReplicationDemandProfile(name);
	  }
	  
	  /**
	   *
	   * @param request
	   * @param sender
	   * @param nodeConfig
	   */
	  @Override
	  public void register(Request request, InetAddress sender, InterfaceGetActiveIPs nodeConfig) 
	  {
		  if (!request.getServiceName().equals(this.name)) 
		  {
			  return;
		  }

	    if (shouldIgnore(request)) {
	      return;
	    }

	    // This happens when called from a reconfigurator
	    if (nodeConfig == null) {
	      return;
	    }
	    this.numRequests++;
	    this.numTotalRequests++;
	    long iaTime = 0;
	    if (lastRequestTime > 0) {
	      iaTime = System.currentTimeMillis() - this.lastRequestTime;
	      this.interArrivalTime = Util.movingAverage(iaTime, interArrivalTime);
	    } else {
	      lastRequestTime = System.currentTimeMillis(); // initialization
	    }

	    if (request instanceof ReplicableRequest
	            && ((ReplicableRequest) request).needsCoordination()) {
	      updateCount++;
	    } else {
	      lookupCount++;
	    }
	    LOG.log(Level.FINE, "%%%%%%%%%%%%%%%%%%%%%%%%%>>> AFTER REGISTER:{0}", this.toString());
	  }
	  
	  /**
	   * Resets the request counters.
	   */
	  public void reset() 
	  {
		  this.interArrivalTime = 0.0;
		  this.lastRequestTime = 0;
		  this.numRequests = 0;
		  this.votesMap = new VotesMap();
		  this.updateCount = 0;
		  this.lookupCount = 0;
	  }
	  
	  public SqrtNReplicationDemandProfile clone() 
	  {
		  return new SqrtNReplicationDemandProfile(this);
	  }
	  
	  /**
	   * Combing of demand profiles
	   * 
	   * @param dp
	   */
	  public void combine(AbstractDemandProfile dp) 
	  {
		  SqrtNReplicationDemandProfile update = (SqrtNReplicationDemandProfile) dp;
		  this.lastRequestTime = Math.max(this.lastRequestTime,
		            update.lastRequestTime);
		  this.interArrivalTime = Util.movingAverage(update.interArrivalTime,
				  this.interArrivalTime, update.getNumRequests());
		  this.numRequests += update.numRequests; // this number is not meaningful at RC
		  this.numTotalRequests += update.numTotalRequests;
		  this.updateCount += update.updateCount;
		  this.lookupCount += update.lookupCount;
		  this.votesMap.combine(update.getVotesMap());
		  LOG.log(Level.FINE, "%%%%%%%%%%%%%%%%%%%%%%%%%>>> AFTER COMBINE:{0}", this.toString());
	  }
	  
	  
	  @Override
	  public void justReconfigured() 
	  {
		  this.lastReconfiguredProfile = this.clone();
		  LOG.log(Level.FINE, "%%%%%%%%%%%%%%%%%%%%%%%%%>>> AFTER CLONE:{0}",
				  this.lastReconfiguredProfile.toString());  
	  }
	  
	  
	  @Override
	  public ArrayList<InetAddress> shouldReconfigure(ArrayList<InetAddress> curActives, 
			  				InterfaceGetActiveIPs nodeConfig) 
	  {
		  if (nodeConfig == null) 
		  {
			  return null;
		  }
		  
		  // we don't want the reconfiguration to happen twice or more
		  if(this.reconfigurationHappened)
		  {
			  return null;
		  }
		  else
		  {
			  ArrayList<InetAddress> activeIPs = nodeConfig.getActiveIPs();
			  
		  }
	  }
	  
	  
	  @Override
	  public boolean shouldReport() 
	  {
		  return !reconfigurationHappened;
	  }
	  
	  /**
	   * Returns the request rate.
	   *
	   * @return the request rate
	   */
	  public double getRequestRate() {
	    return this.interArrivalTime > 0 ? 1.0 / this.interArrivalTime
	            : 1.0 / (this.interArrivalTime + 1000);
	  }

	  /**
	   * Return the number of requests.
	   *
	   * @return the number of requests
	   */
	  public double getNumRequests() {
	    return this.numRequests;
	  }

	  /**
	   * Return the total number of requests.
	   *
	   * @return the total number of requests
	   */
	  public double getNumTotalRequests() {
	    return this.numTotalRequests;
	  }

	  /**
	   * Return the votes map.
	   *
	   * @return the votes map
	   */
	  public VotesMap getVotesMap() {
	    return votesMap;
	  }
	  
	  
	  /**
	   * arun: ignore create, delete, and select commands. We only want to
	   * consider typical read/write commands. Note that select commands are not
	   * expected to have any locality, so they are unlikely to benefit from any
	   * locality based placement.
	   *
	   * @param request
	   * @return true if it should be ignore
	   */
	  private static boolean shouldIgnore(Request request) 
	  {
		  if (!(request instanceof CommandPacket)) 
		  {
			  return true;
		  }
		  // else
		  CommandPacket command = (CommandPacket) request;
		  return command.getCommandType().isCreateDelete()
	            || command.getCommandType().isSelect();
	  }
	  
	  private ArrayList<ArrayList<InetAddress>> createSqrtNPartitionsOfNodes()
	  {
		  
		  
		  
		  return null;
	  }
}