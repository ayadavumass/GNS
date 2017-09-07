package edu.umass.cs.gnsserver.gnsapp.selectnotification;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;

public class EntryPointSelectNotificationState
{
	private final HashMap<Long, Set<InetSocketAddress>> entryPointStateMap;
	private final Object lock;
	private final Random rand;
	
	
	public EntryPointSelectNotificationState()
	{
		entryPointStateMap = new HashMap<Long, Set<InetSocketAddress>>();
		lock = new Object();
		rand = new Random();
	}
	
	/**
	 * The function to add a notification state.
	 * This function is thread-safe.
	 * 
	 * @param forwardedServers
	 * @return
	 * Returns the localHandle for the added 
	 */
	public long addNotificationState(Set<InetSocketAddress> forwardedServers)
	{
		long currHandle = -1;
		synchronized(lock)
		{
			do 
			{
				currHandle = rand.nextLong();	
			} while(entryPointStateMap.containsKey(currHandle));
			
			entryPointStateMap.put(currHandle, forwardedServers);
		}
		return currHandle;
	}
	
	/**
	 * Removes the notification state. 
	 * This function is thread-safe.
	 * 
	 * @return
	 * Returns true on successful removal. 
	 */
	public boolean removeNotificationState()
	{
		return false;
	}
}