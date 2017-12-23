package edu.umass.cs.localtesting;

import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;

public class NonSelfCertifyingGUIDsTest 
{
	private static String ALIAS_PREFIX        = "TestGUID";
	private static String ALIAS_SUFFIX        = "@gns.name";
	
	
	public static void main(String[] args) throws NoSuchAlgorithmException, IOException, ClientException
	{
		int numGuids = Integer.parseInt(args[0]);
		Random rand = new Random();
		ALIAS_PREFIX = rand.nextInt(1024) + ALIAS_PREFIX;
		GNSClient gnsClient = new GNSClient();
		
		KeyPair keyPair = KeyPairGenerator.getInstance(GNSProtocol.RSA_ALGORITHM.toString()).generateKeyPair();
		
		// Creating GUIDs using a single key value pair.
		for(int i=0;i<numGuids; i++)
		{
			String alias = ALIAS_PREFIX+i+ALIAS_SUFFIX;
			gnsClient.execute(GNSCommand.createAccountUsingKeyPair(alias, null, null, keyPair, true));
			GuidEntry guidEntry = GuidUtils.lookupGuidEntryFromDatabase(gnsClient, alias);
			System.out.println("i = "+i+"; alias = "+guidEntry.getEntityName()
			+"; GUID = "+guidEntry.getGuid()+" ; public key="+guidEntry.getPublicKeyString());
		}
		
		
		// Performing write and read operations by reading GuidEntries from the local clientKeyDB
		for(int i=0;i<numGuids; i++)
		{
			String alias = ALIAS_PREFIX+i+ALIAS_SUFFIX;
			GuidEntry guidEntry = GuidUtils.lookupGuidEntryFromDatabase(gnsClient, alias);
			JSONObject json = new JSONObject();
			try 
			{
				json.put("KeyString", "ValueString");
				gnsClient.execute(GNSCommand.update(guidEntry, json));
				JSONObject resultJSON = gnsClient.execute(GNSCommand.read(guidEntry)).getResultJSONObject();
				System.out.println("original json "+json.toString());
				System.out.println("GNS JSON  "+resultJSON.toString());
				
				assert(resultJSON.toString().equals(json.toString()));
			} catch (JSONException e) 
			{
				e.printStackTrace();
			}
		}
		
		// Performing write and read operations by creating GuidEntries using alias and key-value pair.
		for(int i=0;i<numGuids; i++)
		{
			String alias = ALIAS_PREFIX+i+ALIAS_SUFFIX;
			@SuppressWarnings("deprecation")
			GuidEntry guidEntry = GuidUtils.getGuidEntryFromAliasAndKeyPair(gnsClient.getGNSProvider(), alias, keyPair);
			JSONObject json = new JSONObject();
			try 
			{
				json.put("KeyString1", "ValueString1");
				gnsClient.execute(GNSCommand.update(guidEntry, json));
				JSONObject resultJSON = gnsClient.execute(GNSCommand.read(guidEntry)).getResultJSONObject();
				System.out.println("original json "+json.toString());
				System.out.println("GNS JSON  "+resultJSON.toString());
				assert(resultJSON.toString().equals(json.toString()));
			} catch (JSONException e) 
			{
				e.printStackTrace();
			}
		}
		
		gnsClient.close();
	}
}