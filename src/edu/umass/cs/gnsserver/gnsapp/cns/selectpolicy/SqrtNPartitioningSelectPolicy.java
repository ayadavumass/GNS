package edu.umass.cs.gnsserver.gnsapp.cns.selectpolicy;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;

import edu.umass.cs.gnsserver.gnsapp.cns.common.NodePartitioning;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectRequestPacket;

/**
 * This class implements the sqrt(n) partitioning select policy. 
 * The policy performs the following steps on each call of getNodesForSelectRequest method.
 * The policy retrieves the current set of active replicas by reading the gns config file, 
 * which the gigapaxos updates when active replicas change, and partitions 
 * the set of active replicas as follows. The policy partitions the total set
 * of active replicas deterministically into sqrt(n) partitions of sqrt(n) nodes each. 
 * After partitioning, the policy picks one partition at random and returns all nodes
 * corresponding to that partitions as the return value for  getNodesForSelectRequest call.
 *
 * The above sequence of steps happen on each call of getNodesForSelectRequest method so that 
 * the policy returns the most up-to-date partitioning of active replicas. 
 * 
 * @author ayadav
 *
 */
public class SqrtNPartitioningSelectPolicy extends AbstractSelectPolicy
{
	private Random rand;
	public SqrtNPartitioningSelectPolicy()
	{
		super();
		rand = new Random();
		
	}
	@Override
	public Set<InetSocketAddress> getNodesForSelectRequest(SelectRequestPacket selectreq) 
	{
		Set<InetSocketAddress> currentActives = fetchCurrentActives();
		int sqrtn = (int)Math.sqrt(currentActives.size());
		
		LOG.log(Level.FINE, "Current set of total actives {0}", new Object[]{currentActives});
		ArrayList<ArrayList<InetSocketAddress>> partitions =  
							NodePartitioning.createPartitions(currentActives, sqrtn);
		
		LOG.log(Level.FINE, "Partitioning info {0}", new Object[]{partitions});
		
		Set<InetSocketAddress> snodes = new HashSet<InetSocketAddress>();
		
		ArrayList<InetSocketAddress> plist = partitions.get(rand.nextInt(partitions.size()));
		snodes.addAll(plist);
		LOG.log(Level.FINE, "Returned set of nodes {0}", new Object[]{snodes});
		return snodes;
	}
	
	public static void main(String[] args)
	{
		try 
		{
			Class<?> spc = Class.forName("edu.umass.cs.gnsserver.gnsapp.cns.selectpolicy.SqrtNPartitioningSelectPolicy");
			AbstractSelectPolicy selectObj = AbstractSelectPolicy.createSelectPolicy(spc);
			Set<InetSocketAddress> sockAddr = selectObj.getNodesForSelectRequest(null);
			
			System.out.println("sockAddr "+sockAddr);
		} 
		catch (ClassNotFoundException e) 
		{
			e.printStackTrace();
		}
	}
}