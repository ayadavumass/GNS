package edu.umass.cs.gnsserver.gnsapp.demandprofiles;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.InterfaceGetActiveIPs;


/**
 * This demand profile uses the partition information from ConsistentReconfigurableNodeConfigWithSlicing
 * and creates one active replica for a service name, GUID in GNS, in each of the partition. 
 * In a partition, the demand profile picks a node for a GUID by hashing the GUID to one of the 
 * nodes in the partition. 
 * 
 * If there are no changes to the NodeConfig, then on repeated calls of the shouldReconfigure 
 * method, this demand profile returns the same set of active replicas. If the reconfigure in place
 * is turned off, which it should be when this demand profile is used, then the reconfigurators 
 * don't perform any reconfigurations. But if the NodeConfig changes, then the shouldReconfigure returns 
 * a different set of active replicas. To minimize the change in the set, this demand profile uses
 * ConsistentHashing mechanism to map a GUID to a node within a partition. 
 * @author ayadav
 */
public class SqrtNReplicationDemandProfile extends AbstractDemandProfile 
{
	private static final Logger LOG = Logger.getLogger(SqrtNReplicationDemandProfile.class.getName());
	
	/**
	 * The keys for the demand profile packet.
	 */
	private enum Keys 
	{
		/**
		 * SERVICE_NAME
		 */
		SERVICE_NAME,
	  };
	  
	  private SqrtNReplicationDemandProfile lastReconfiguredProfile = null;
	  
	  
	  public SqrtNReplicationDemandProfile(String name) 
	  {
		  super(name);
		  LOG.log(Level.FINE, "SqrtNReplicationDemandProfile(String name) constructor called for "+name);
	  }
	  
	  /**
	   * Create a SqrtNReplicationDemandProfile instance by making 
	   * a deep copy of another instance.
	   *
	   * @param dp
	   */
	  public SqrtNReplicationDemandProfile(SqrtNReplicationDemandProfile dp) 
	  {
	    super(dp.name);
	    LOG.log(Level.FINE, "SqrtNReplicationDemandProfile(SqrtNReplicationDemandProfile dp) "
	    		+ " constructor called "+name);
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
		  LOG.log(Level.FINE,"SqrtNReplicationDemandProfile(JSONObject json) "
		  		+ "constructor called for "+name);
	  }
	  
	  /**
	   *
	   * @return the stats
	   */
	  @Override
	  public JSONObject getStats() 
	  {  
		  JSONObject json = new JSONObject();
		  try
		  {
			  json.put(Keys.SERVICE_NAME.toString(), this.name);
		  } 
		  catch (JSONException je) 
		  {
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
		  LOG.log(Level.FINE, "createDemandProfile called for "+name);
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
		  LOG.log(Level.FINE, "SqrtNReplicationDemandProfile register called for "+this.name
				  +"nodeconfig "+ ((nodeConfig!=null)?nodeConfig:"nodeConfig null"));
		  
		  assert(nodeConfig != null);
		  
		  if (!request.getServiceName().equals(this.name)) 
		  {
			  return;
		  }
		  
		  if (shouldIgnore(request)) 
		  {
			  return;
		  }
	  }
	  
	  public SqrtNReplicationDemandProfile clone() 
	  {
		  LOG.log(Level.FINE, "clone() called for "+this.name);
		  return new SqrtNReplicationDemandProfile(this);
	  }
	  
	  /**
	   * Combing of demand profiles 
	   * 
	   * @param dp
	   */
	  public void combine(AbstractDemandProfile dp) 
	  {
		  LOG.log(Level.FINE, "Combine called for "+this.name);
	  }
	  
	  @Override
	  public void justReconfigured() 
	  {
		  LOG.log(Level.FINE, "justReconfigured called for "+this.name);
		  this.lastReconfiguredProfile = this.clone();
		  LOG.log(Level.FINE, "%%%%%%%%%%%%%%%%%%%%%%%%%>>> AFTER CLONE:{0}",
				  this.lastReconfiguredProfile.toString());  
	  }
	  
	  @Override
	  public ArrayList<InetAddress> shouldReconfigure(ArrayList<InetAddress> curActives, 
			  				InterfaceGetActiveIPs nodeConfig)
	  {
		  LOG.log(Level.FINE, this.name + " SqrtNReplicationDemandProfile "
		  		+ "shouldReconfigure called "
				+ ((nodeConfig!=null)?nodeConfig.getActiveIPs():"nodeConfig null")
				+ ((curActives!=null)?("currActive "+curActives):"curActives null") );
		  
		  assert(nodeConfig != null);
		  return null;
		  /*if(this.reconfigurationHappened)
		  {
			  LOG.log(Level.FINE, this.name + " SqrtNReplicationDemandProfile shouldReconfigure "
			  		+ "	called reconfiguration already happened curActives ");
			  return null;
		  }
		  else
		  {
			  ArrayList<ArrayList<String>> partitionlist = createSqrtNPartitionsOfNodes(nodeConfig);
			  String guidToReconfigure = this.name;
			  ArrayList<InetAddress> actives =  createActiveReplicasForGUID
					  								(guidToReconfigure, partitionlist);
			  this.reconfigurationHappened = true;
			  
			  LOG.log(Level.FINE, "%%%%%%%%%%%%%%%%%%%%%%%%%>>> shouldReconfigure: GUID {0}, "
			  				+ "new actives {1} "
					  		, new Object[]{this.name, actives});
			  
			  LOG.log(Level.FINE, "shouldReconfigure "+this.name+" "+actives);
			  
			  return actives;
		  }*/
	  }
	  
	  
	  @Override
	  public boolean shouldReport() 
	  {
		  LOG.log(Level.FINE, "shouldReport called for "+this.name);
		  return true;
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
		  return false;
	  }
	  
	  private ArrayList<ArrayList<String>> createSqrtNPartitionsOfNodes(
			  						InterfaceGetActiveIPs nodeConfig)
	  {
		  LOG.log(Level.FINE, this.name + " createSqrtNPartitionsOfNodes called ");
		  
		  ArrayList<String> nodesString = new ArrayList<String>();
		  
		  //convert ip addresses into string for sorting.
		  for(int i=0; i<nodeConfig.getActiveIPs().size(); i++)
		  {
			  nodesString.add(nodeConfig.getActiveIPs().get(i).getHostAddress());
		  }
		  
		  Collections.sort(nodesString);
		  
		  
		  int sqrtn = (int)Math.sqrt(nodesString.size());
		  ArrayList<ArrayList<String>> partitionlist 
		  								= new ArrayList<ArrayList<String>>();
		  
		  for(int i=0; i<sqrtn; i++)
		  {
			  ArrayList<String> partition = new ArrayList<String>();
			  partitionlist.add(partition);
		  }
		  
		  
		  for(int i=0; i<nodesString.size(); i++)
		  { 
			  int partNum = i%sqrtn;
			  String inetAddrString = nodesString.get(i);
			  
			  partitionlist.get(partNum).add(inetAddrString);
		  }
		  
		  for(int i=0; i<sqrtn; i++)
		  {
			  Collections.sort(partitionlist.get(i));
		  }
		  LOG.log(Level.FINE, "Node partitions "+partitionlist);
		  return partitionlist;
	  }
	  
	  
	  private ArrayList<InetAddress> createActiveReplicasForGUID
	  									(String GUID, ArrayList<ArrayList<String>> partitionlist)
	  {
		  ArrayList<InetAddress> activeReplicas = new ArrayList<InetAddress>();
		  for(int i=0; i<partitionlist.size(); i++)
		  {
			  ArrayList<String> partition = partitionlist.get(i);
			  int index = Utils.consistentHashAString(GUID, partition.size());
			  String currActiveIPStr = partition.get(index);
			  
			  try 
			  {
				  activeReplicas.add(InetAddress.getByName(currActiveIPStr));
			  } catch (UnknownHostException e) 
			  {
				// It is a standard conversion from dot notation IP String to InetAdress,
				// so there should never be exception here.
				e.printStackTrace();
			  }
		  }
		  return activeReplicas;
	  }
	  
	  private static class SampleNodeConfig implements InterfaceGetActiveIPs
	  {
		  private final ArrayList<InetAddress> nodeIPList;
		  
		  public SampleNodeConfig(ArrayList<InetAddress> nodeIPList)
		  {
			  this.nodeIPList = nodeIPList;
		  }
		  
		  @Override
		  public ArrayList<InetAddress> getActiveIPs() 
		  {
			  return nodeIPList;
		  }
	  }
	  
	  public static void main(String[] args)
	  {
		  int numNodes = 16;
		  String guid = Utils.getSHA1("myGUID10");
		  
		  System.out.println("GUID "+guid);
		  ArrayList<InetAddress> nodeIPList = new ArrayList<InetAddress>();
		  
		  for(int i=0; i<numNodes; i++)
		  {
			  String ipAddress = "10.1.1."+(2+i);
			  
			  try
			  {
				  nodeIPList.add(InetAddress.getByName(ipAddress));
			  }
			  catch (UnknownHostException e)
			  {
				  e.printStackTrace();
			  }
		  }
		  
		  InterfaceGetActiveIPs samplenc = new SampleNodeConfig(nodeIPList);
		  
		  SqrtNReplicationDemandProfile dp = new SqrtNReplicationDemandProfile(guid);
		  
		  ArrayList<ArrayList<String>> partitionlist = dp.createSqrtNPartitionsOfNodes(samplenc);
		  
		  System.out.println("partitionlist "+partitionlist);
		  
		  ArrayList<InetAddress> actives = dp.shouldReconfigure(null, samplenc);
		  assert(actives != null);
		  
		  System.out.println("actives "+actives);
		  
		  
		  actives = dp.shouldReconfigure(null, samplenc);
		  assert(actives == null);
	  }
}