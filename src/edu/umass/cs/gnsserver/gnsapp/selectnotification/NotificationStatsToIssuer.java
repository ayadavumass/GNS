package edu.umass.cs.gnsserver.gnsapp.selectnotification;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * This class represents the notification stats for a 
 * select request that is sent back to an issuer.
 * An issuer can be another an entry point name server, whic forwards requests
 * to another name servers, or the GNSClient. 
 * 
 * @author ayadav
 *
 */
public class NotificationStatsToIssuer 
{
	public static final String TOTAL_NOTIFICATIONS = "TOTAL_NOTIFICATIONS";
	public static final String FAILED_NOTIFICATIONS = "FAILED_NOTIFICATIONS";
	public static final String PENDING_NOTIFICATIONS = "PENDING_NOTIFICATIONS";
	
	
	private final long totalNotifications;
	private final long failedNotifications;
	private final long pendingNotifications;
	
	
	public NotificationStatsToIssuer(long totalNotifications, 
						long failedNotifications, long pendingNotifications)
	{
		this.totalNotifications = totalNotifications;
		this.failedNotifications = failedNotifications;
		this.pendingNotifications = pendingNotifications;
	}
	
	public long getTotalNotifications()
	{
		return this.totalNotifications;
	}
	
	public long getFailedNotifications()
	{
		return this.failedNotifications;
	}
	
	public long getPendingNotifications()
	{
		return this.pendingNotifications;
	}
	
	public JSONObject toJSONObject() throws JSONException
	{
		JSONObject json = new JSONObject();
		json.put(TOTAL_NOTIFICATIONS, totalNotifications);
		json.put(FAILED_NOTIFICATIONS, failedNotifications);
		json.put(PENDING_NOTIFICATIONS, pendingNotifications);
		return json;
	}
	
	public static NotificationStatsToIssuer fromJSON(JSONObject json) throws JSONException
	{
		long totalNot = json.getLong(TOTAL_NOTIFICATIONS);
		long failedNot = json.getLong(FAILED_NOTIFICATIONS);
		long pendingNot = json.getLong(PENDING_NOTIFICATIONS);
		
		return new NotificationStatsToIssuer(totalNot, failedNot, pendingNot);
	}
}