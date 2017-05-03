package edu.umass.cs.gnsserver.gnsapp.cns.selectpolicy;

import java.net.InetSocketAddress;
import java.util.Set;

import edu.umass.cs.gnsserver.gnsapp.packet.SelectRequestPacket;


/**
 * This class implements the replicate all select policy, i.e., a select request
 * goes to all nodes. 
 * @author ayadav
 *
 */
public class ReplicateAllSelectPolicy extends AbstractSelectPolicy
{
	@Override
	public Set<InetSocketAddress> getNodesForSelectRequest(SelectRequestPacket selectreq) 
	{
		return this.fetchCurrentActives();
	}
}