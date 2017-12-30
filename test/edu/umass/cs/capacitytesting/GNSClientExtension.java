package edu.umass.cs.capacitytesting;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;

/**
 * This client extends GNSClient to issue some requests that are not 
 * yet supported by the GNSClient. Currently, the only request this client 
 * issues is to request the active replicas for a name. 
 * 
 * The experiment code requests active replicas for a name to check 
 * if the GNS has the expected set of actives for a name.
 * 
 * @author ayadav
 */
public class GNSClientExtension extends GNSClient
{
	public GNSClientExtension() throws IOException 
	{
		super();
	}
	
	
	public Set<InetSocketAddress> getActivesForName(String name)
	{
		BlockingRequestCallback callback = new BlockingRequestCallback(0);
		try {
			this.asyncClient.sendReconfigurationRequest(
					ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS, name, null, callback);
			Request response = callback.getResponse();
			if(response instanceof RequestActiveReplicas)
			{
				return ((RequestActiveReplicas)response).getActives();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	/**
	 *
	 */
	public final class BlockingRequestCallback implements RequestCallback 
	{
		Request response = null;
		final Long timeout;
		
		BlockingRequestCallback(long timeout) {
			this.timeout = timeout;
		}
		
		Request getResponse() {
			synchronized (this) {
				if (this.response == null)
					try {
							this.wait(this.timeout);
						} catch (InterruptedException e) {
							e.printStackTrace();
							// continue waiting
						}
				}
			return this.response;
		}

		@Override
		public void handleResponse(Request response) 
		{
			this.response = response;
			synchronized (this) {
				this.notify();
			}
		}
	}
	
	public static void main(String[] args)
	{
		// FIXME: some testing code.
	}
}