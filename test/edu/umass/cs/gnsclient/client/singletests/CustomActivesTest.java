package edu.umass.cs.gnsclient.client.singletests;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;


import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.utils.Util;
import edu.umass.cs.utils.Utils;

/**
 * This class tests the creation of names with custom actives. 
 * @author ayadav
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CustomActivesTest extends DefaultGNSTest
{
	private GNSClientExtension gnsClientExten;
	private Set<InetSocketAddress> allActives;
	private static final String ACCOUNT_ALIAS = "customActiveAccount"+RandomString.randomString(12);
	private GuidEntry accountGuidEntry;
	
	public CustomActivesTest()
	{
		super();
		try {
			gnsClientExten = new GNSClientExtension();
			gnsClientExten.setNumRetriesUponTimeout(2).setForcedTimeout(TIMEOUT);
			// requesting all actives.
			allActives = gnsClientExten.getActivesForName("**");
			Assert.assertNotEquals(allActives, null);
		} catch (IOException e) {
			Utils.failWithStackTrace("Exception when we were not expecting it in "
					+ " CustomActivesTest() of CustomActivesTest", e);
		}
		
	}
	
	
	@SuppressWarnings("deprecation")
	@Test
	public void test_0_createAccountGUIDWithCustomActive()
	{
		InetSocketAddress active = (InetSocketAddress) Util.selectRandom(allActives);
		Set<InetSocketAddress> actives = new HashSet<InetSocketAddress>();
		actives.add(active);
		
		try {
			client.execute(GNSCommand.createAccount(ACCOUNT_ALIAS, "password", actives));
			// supressing waning because of deprecated GNSProvider
			accountGuidEntry = GuidUtils.lookupGuidEntryFromDatabase
										(GNSClient.getGNSProvider(), ACCOUNT_ALIAS);
			
			Set<InetSocketAddress> aliasActives = gnsClientExten.getActivesForName(ACCOUNT_ALIAS);
			Set<InetSocketAddress> guidActives = gnsClientExten.getActivesForName(accountGuidEntry.getGuid());
			
			System.out.println("aliasActives="+aliasActives+" ; guidActives="+guidActives+
					" ; actives="+actives);
			
			Assert.assertEquals(aliasActives.size(), 1);
			Assert.assertEquals(guidActives.size(), 1);
			Assert.assertTrue(aliasActives.contains(active));
			Assert.assertTrue(guidActives.contains(active));
		} catch (ClientException | NoSuchAlgorithmException | IOException e) {
			Utils.failWithStackTrace("Exception when we were not expecting it in "
					+ " test_0_createAccountGUIDWithCustomActive of CustomActivesTest", e);
		}
		
		
	}
	
	@After
	public void removeGUIDs()
	{
		System.out.println("Removing account GUID " + this.accountGuidEntry+" in test CustomActivesTest");
		try {
			client.execute(GNSCommand.accountGuidRemove(accountGuidEntry));
		} catch (ClientException | IOException e) {
			Utils.failWithStackTrace("Exception when we were not expecting it in "
					+ " cleanup of CustomActivesTest", e);
		}
		gnsClientExten.close();
	}
	
	
	private class GNSClientExtension extends GNSClient
	{
		public GNSClientExtension() throws IOException 
		{
			super();
		}
		
		
		public Set<InetSocketAddress> getActivesForName(String name)
		{
			BlockingRequestCallback callback = new BlockingRequestCallback(TIMEOUT);
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
		
	}
}