package edu.umass.cs.localtesting;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

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
				GuidEntry guidEntry = GuidUtils.getGuidEntryFromAliasAndKeyPair(
						gnsClient.getGNSProvider(), alias, keyPair);
				json.put("KeyString", "ValueString");
				gnsClient.execute(GNSCommand.update(guidEntry, json));
			} catch (JSONException | ClientException | IOException e) 
			{
				e.printStackTrace();
			}
		}
	}
	
	private static void saveKeyPairToFile(String path, KeyPair keyPair) throws IOException
	{
		PrivateKey privateKey = keyPair.getPrivate();
		PublicKey publicKey = keyPair.getPublic();
		
		// Store public Key
		X509EncodedKeySpec x509EncodedKeySpec = new X509EncodedKeySpec(publicKey.getEncoded());
		FileOutputStream fos = new FileOutputStream(path + "/public.key");
		fos.write(x509EncodedKeySpec.getEncoded());
		fos.close();
		
		// Store private key
		PKCS8EncodedKeySpec pkcs8EncodedKeySpec = new PKCS8EncodedKeySpec(privateKey.getEncoded());
		fos = new FileOutputStream(path + "/private.key");
		fos.write(pkcs8EncodedKeySpec.getEncoded());
		fos.close();
	}
	
	private static KeyPair loadKeyPairFromFile(String path, String algorithm) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException
	{
		// Read public key
		File filePublicKey = new File(path + "/public.key");
		FileInputStream fis = new FileInputStream(path + "/public.key");
		byte[] encodedPublicKey = new byte[(int) filePublicKey.length()];
		fis.read(encodedPublicKey);
		fis.close();
		
		// Read private key.
		File filePrivateKey = new File(path + "/private.key");
		fis = new FileInputStream(path + "/private.key");
		byte[] encodedPrivateKey = new byte[(int) filePrivateKey.length()];
		fis.read(encodedPrivateKey);
		fis.close();
		
		// Generate keypair
		KeyFactory keyFactory = KeyFactory.getInstance(algorithm);
		X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(encodedPublicKey);
		PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);
		PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(encodedPrivateKey);
		PrivateKey privateKey = keyFactory.generatePrivate(privateKeySpec);
		return new KeyPair(publicKey, privateKey);		
	}
	
	
	public static void main(String[] args) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException
	{
		int operationType = Integer.parseInt(args[0]);
		int numGuids = Integer.parseInt(args[1]);
		// Directory path to store or retrieve public-private key pair.
		String keyDir = args[2];
		
		gnsClient = new GNSClient();
		
		if(operationType == GUID_CREATION)
		{
			keyPair = KeyPairGenerator.getInstance(GNSProtocol.RSA_ALGORITHM.toString()).generateKeyPair();
			saveKeyPairToFile(keyDir, keyPair);
			createGUIDs(numGuids);
		}
		else if(operationType == GUID_OPERAITON)
		{
			keyPair = loadKeyPairFromFile(keyDir, GNSProtocol.RSA_ALGORITHM.toString());
			guidOperation(numGuids);
		}
		gnsClient.close();
	}
}