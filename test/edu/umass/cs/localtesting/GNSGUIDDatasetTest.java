package edu.umass.cs.localtesting;

import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;


public class GNSGUIDDatasetTest 
{
	public static final int GUID_CREATION			= 1;
	public static final int GUID_OPERAITON			= 2;
	
	private static GNSClient gnsClient;
	private static KeyPair keyPair;
	private static final String ALIAS_PREFIX        = "TestGUID";
	private static final String ALIAS_SUFFIX        = "@gns.name";
	
	private static void createGUIDs(int numGuids)
	{
		for(int i=0; i<numGuids; i++)
		{
			String alias = ALIAS_PREFIX + i + ALIAS_SUFFIX;
			try 
			{
				gnsClient.execute(GNSCommand.createAccountUsingKeyPair(alias, null, null, keyPair, false));
			} catch (ClientException | NoSuchAlgorithmException | IOException e) 
			{
				System.out.println("GUID creation for alias="+alias+" failed.");
				e.printStackTrace();
			}
		}
	}
	
	
	private static void guidOperation(int numGuids)
	{
		for(int i=0; i<numGuids; i++)
		{
			String alias = ALIAS_PREFIX + i + ALIAS_SUFFIX;
			JSONObject json = new JSONObject();
			try 
			{
				@SuppressWarnings("deprecation")
				GuidEntry guidEntry = GuidUtils.getGuidEntryFromAliasAndKeyPair(gnsClient.getGNSProvider(), alias, keyPair);
				json.put("KeyString", "ValueString");
				gnsClient.execute(GNSCommand.update(guidEntry, json));
			} catch (JSONException | ClientException | IOException e) 
			{
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws IOException, NoSuchAlgorithmException
	{
		int operationType = Integer.parseInt(args[0]);
		int numGuids = Integer.parseInt(args[1]);
		
		gnsClient = new GNSClient();
		keyPair = KeyPairGenerator.getInstance(GNSProtocol.RSA_ALGORITHM.toString()).generateKeyPair();
		
		if(operationType == GUID_CREATION)
		{
			createGUIDs(numGuids);
		}
		else if(operationType == GUID_OPERAITON)
		{
			guidOperation(numGuids);
		}
	}
}