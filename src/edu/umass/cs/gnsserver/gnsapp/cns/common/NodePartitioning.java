package edu.umass.cs.gnsserver.gnsapp.cns.common;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * This class provides static utility methods to partition 
 * a set of nodes into a specified number of partitions deterministically.
 * 
 * By deterministically, we mean if the set of nodes, irrespective of order, 
 * is same then the returned set of partitions are same. 
 * 
 * @author ayadav
 */
public class NodePartitioning 
{
	private static final Logger LOG = Logger.getLogger(NodePartitioning.class.getName());
	
	/**
	 * Partitions the totalActiveReplicas into numPartitions by sorting 
	 * the totalActiveReplicas set and then partitioning into numPartitions, so that
	 * the partitions are created deterministically.
	 * 
	 * @param totalActiveReplicas
	 * @param numPartitions
	 * @return
	 */
	public static ArrayList<ArrayList<InetSocketAddress>> createPartitions(
						Set<InetSocketAddress> totalActiveReplicas, int numPartitions)
	{	
		ArrayList<String> nodesString = new ArrayList<String>();
		
		Iterator<InetSocketAddress> setIter = totalActiveReplicas.iterator();
		
		while(setIter.hasNext())
		{
			InetSocketAddress sockAdd = setIter.next();
			nodesString.add(sockAdd.getAddress().getHostAddress()+":"+sockAdd.getPort());
		}
		
		Collections.sort(nodesString);
		
		ArrayList<ArrayList<InetSocketAddress>> partitionlist 
					= new ArrayList<ArrayList<InetSocketAddress>>();
		
		for(int i=0; i<numPartitions; i++)
		{
			ArrayList<InetSocketAddress> partition = new ArrayList<InetSocketAddress>();
			partitionlist.add(partition);
		}
		
		for(int i=0; i<nodesString.size(); i++)
		{ 
			int partNum = i%numPartitions;
			String inetAddrString = nodesString.get(i);
			String [] parsed = inetAddrString.split(":");
			InetSocketAddress sock;
			try {
				sock = new InetSocketAddress(InetAddress.getByName(parsed[0]),
																Integer.parseInt(parsed[1]));
				partitionlist.get(partNum).add(sock);
			} catch (NumberFormatException | UnknownHostException e) 
			{
				// this is just a re conversion of an already existing 
				// socket address. This exception should never occur
				e.printStackTrace();
			}
		}		
		LOG.log(Level.FINE, "Node partitions "+partitionlist);
		return partitionlist;
	}
	
	public static void main(String[] args)
	{
		
		
		
	}
}