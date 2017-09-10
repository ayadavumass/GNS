package edu.umass.cs.gnsserver.gnsapp.selectnotification;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * This class stores the select notifications state at a
 * name server.
 * 
 * @author ayadav
 *
 */
public class NameServerSelectNotificationState 
{
	private final HashMap<Long, List<NotificationSendingStats>> notificationInfo;
	
	private final Object lock;
	private final Random rand;
	
	public NameServerSelectNotificationState()
	{
		this.notificationInfo = new HashMap<Long, List<NotificationSendingStats>>();
		lock = new Object();
		rand = new Random();
	}
	
	/**
	 * Stores {@code stats} for the given {@code localHandle}.
	 * This function is thread-safe.
	 * 
	 * @param localHandle
	 * @param stats
	 */
	public void addNotificationStats(long localHandle, NotificationSendingStats stats)
	{
		synchronized(lock)
		{
			notificationInfo.get(localHandle).add(stats);
		}
	}
	
	
	public List<NotificationSendingStats> lookupNotificationStats(long localHandle)
	{
		return notificationInfo.get(localHandle);
	}
	
	public List<NotificationSendingStats> removeNotificationInfo(long localHandle)
	{
		synchronized(lock)
		{
			return notificationInfo.remove(localHandle);
		}
	}
	
	
	public long getUniqueIDAndInit()
	{
		long reqId = -1;
		synchronized(lock)
		{
			do
			{
				reqId = rand.nextLong();
			}while(notificationInfo.containsKey(reqId));
			notificationInfo.put(reqId, new LinkedList<NotificationSendingStats>());
		}
		return reqId;
	}
	
	
}