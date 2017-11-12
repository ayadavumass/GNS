package edu.umass.cs.localtesting;

import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;

public class NonSelfCertifyingGUIDsTest 
{
	public static void main(String[] args) throws NoSuchAlgorithmException, IOException, ClientException
	{
		GNSClient gnsClient = new GNSClient();
		
		KeyPair keyPair = KeyPairGenerator.getInstance(GNSProtocol.RSA_ALGORITHM.toString()).generateKeyPair();
		
		for(int i=0;i<10; i++)
		{
			String alias = "GUID"+i+"@gns.name";
			gnsClient.execute(GNSCommand.createAccountUsingKeyPair(alias, null, null, keyPair, true));
			
			GuidEntry guidEntry = GuidUtils.lookupGuidEntryFromDatabase(gnsClient, alias);
			System.out.println("i = "+i+"; alias = "+guidEntry.getEntityName()
			+"; GUID = "+guidEntry.getGuid()+" ; public key="+guidEntry.getPublicKeyString());
		}
	}
}