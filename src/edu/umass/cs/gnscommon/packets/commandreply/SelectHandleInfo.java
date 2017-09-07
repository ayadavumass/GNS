package edu.umass.cs.gnscommon.packets.commandreply;

import java.net.InetSocketAddress;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * This class represents a handle for select notifications. 
 * 
 * @author ayadav
 *
 */
public class SelectHandleInfo
{
	public static enum Keys
	{
		/**
		 * A handleId corresponding to this handle. 
		 */
		HANDLE_ID,
		/**
		 * The address of name server/active that sent this message.
		 */
		SERVER_ADDRESS,
	}
	
	/**
	 * <serverAddress, handleId> is a unique identifier for a handle. 
	 */
	private final long handleId;
	
	private final InetSocketAddress serverAddress;
	
	
	public SelectHandleInfo(long handleId, InetSocketAddress serverAddress)
	{
		this.handleId = handleId;
		this.serverAddress = serverAddress;
	}
	
	
	public JSONObject toJSONObject() throws JSONException
	{
		JSONObject json = new JSONObject();
		json.put(Keys.HANDLE_ID.toString(), handleId);
		json.put(Keys.SERVER_ADDRESS.toString(), 
					serverAddress.getAddress().getHostAddress()+":"+serverAddress.getPort());
		return json;
	}
	
	
	public static SelectHandleInfo fromJSONObject(JSONObject json) throws JSONException
	{
		long handle = json.getLong(Keys.HANDLE_ID.toString());
		String[] ipPort = json.getString(Keys.SERVER_ADDRESS.toString()).split(":");
		
		InetSocketAddress serverAdd = new InetSocketAddress
							(ipPort[0], Integer.parseInt(ipPort[1]));
		
		return new SelectHandleInfo(handle, serverAdd);
	}
}