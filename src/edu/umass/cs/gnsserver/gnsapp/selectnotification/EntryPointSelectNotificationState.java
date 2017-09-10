package edu.umass.cs.gnsserver.gnsapp.selectnotification;

import java.util.HashMap;
import java.util.List;
import java.util.Random;


import edu.umass.cs.gnscommon.packets.commandreply.SelectHandleInfo;

/**
 * This class is used to store the select notification state 
 * at an entry-point name server.
 * 
 * @author ayadav
 */
public class EntryPointSelectNotificationState
{
	/**
	 * The key in this map is the localHandleId at the entry-point name server.
	 * The value, List<SelectHandleInfo>, is the list of SelectHandleInfos corresponding 
	 * to all name servers where the earlier issued selectAndNotify request was forwarded to.
	 * Each SelectHandleInfo contains the server address of its name server and the localHandleId
	 * at that name server.
	 */
	private final HashMap<Long, List<SelectHandleInfo>> entryPointStateMap;
	private final Object lock;
	private final Random rand;
	
	
	public EntryPointSelectNotificationState()
	{
		entryPointStateMap = new HashMap<Long, List<SelectHandleInfo>>();
		lock = new Object();
		rand = new Random();
	}
	
	
	/**
	 * The function to add a notification state.
	 * This function is thread-safe.
	 * 
	 * @param selectHandleList
	 * The list of select handles at the name servers where the  
	 * issued selectAndNotify request was forwarded.
	 * 
	 * @return
	 * Returns the localHandleId for the added select handle
	 */
	public long addNotificationState(List<SelectHandleInfo> selectHandleList)
	{
		long currHandle = -1;
		synchronized(lock)
		{
			do 
			{
				currHandle = rand.nextLong();	
			} while(entryPointStateMap.containsKey(currHandle));
			entryPointStateMap.put(currHandle, selectHandleList);
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
	
	
	public List<SelectHandleInfo> getListOfHandles(long localHandleId)
	{
		return entryPointStateMap.get(localHandleId);
	}
}