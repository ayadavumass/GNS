
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.Random;

import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.packets.CommandPacket;

public class SimpleGNSClientExample 
{
	// replace with your account alias
	private static String ACCOUNT_ALIAS_PREFIX = "admin";
	private static String ACCOUNT_ALIAS_SUFFIX = "@gns.name";
	//private static String ACCOUNT_ALIAS = "admin@gns.name";
	private static GNSClient client;

	/**
	 * @param args
	 * @throws IOException
	 * @throws InvalidKeySpecException
	 * @throws NoSuchAlgorithmException
	 * @throws ClientException
	 * @throws InvalidKeyException
	 * @throws SignatureException
	 * @throws Exception
	 */
	public static void main(String[] args) throws IOException,
				InvalidKeySpecException, NoSuchAlgorithmException, ClientException,
				InvalidKeyException, SignatureException, Exception 
	{
		int numGuids = Integer.parseInt(args[0]);
		
		client = new GNSClient();
		System.out.println("[Client connected to GNS]\n");
		client = client.setForcedTimeout(10000);
		client = client.setNumRetriesUponTimeout(5);
		try
		{
			for(int i=0; i<numGuids; i++)
			{
				System.out.println("// account GUID creation\n"
						+ "GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS,"
						+ " \"password\", true)");
				
				String alias = ACCOUNT_ALIAS_PREFIX+i+ACCOUNT_ALIAS_SUFFIX;
				
				client.execute(GNSCommand.createAccount(alias));
				GuidEntry guid = GuidUtils.getGUIDKeys(alias);
				System.out.println("GUID num i "+i+" guid "+guid.getGuid()+" created");
				
				for(int j=0; j<1; j++)
				{
					// Change a field
					//client.execute(GNSCommand.update(guid, (new JSONObject()).put(j+"", j+"val")));
					//System.out.println("Performing update guid "+guid.getGuid()+"j "+j);
				}
				//Thread.sleep(1000);
			}

			Random rand = new Random();
			for(int i=0; i<numGuids; i++)
			{
				System.out.println("// account GUID creation\n"+ "GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS,"+ " \"password\", true)");
				
				String alias = ACCOUNT_ALIAS_PREFIX+i+ACCOUNT_ALIAS_SUFFIX;
				
				//client.execute(GNSCommand.createAccount(alias));
				
				GuidEntry guid = GuidUtils.getGUIDKeys(alias);
				
				System.out.println("GUID num i "+i+" guid "+guid.getGuid()+" updating");
				
				for(int j=0; j<10; j++)
				{
					// Change a field
					JSONObject json = new JSONObject();
					json.put("a"+j, rand.nextInt(100));
					
					client.execute(GNSCommand.update(guid, json));
					
					
					System.out.println("Performing update guid "+guid.getGuid()+"j "+j);
				}
				//Thread.sleep(1000);
			}
			
			// query guids now
			String query = "$and:[(\"~a0\":($gt:0, $lt:100)),(\"~a1\":($gt:0, $lt:100))]";
			System.out.println("Sending select query");
			CommandPacket response = client.execute(GNSCommand.selectQuery(query));
			System.out.print("result size "+response.getResultList().size());
			
		}
		catch (Exception | Error e) 
		{
			System.out.println("Exception during accountGuid creation: " + e);
			e.printStackTrace();
			System.exit(1);
		}
		
		client.close();
		System.out.println("\nclient.close() // test successful");
	}
}
