package edu.umass.cs.gnsserver.gnsapp.nonblockingselect.commands;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupSetupQuery;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupSetupQueryWithGuid;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupSetupQueryWithGuidAndInterval;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectNear;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectQuery;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectWithin;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectRequestPacket;

public abstract class AbstractNonBlockingSelectCmd
{
	protected static HashMap<String, String> classMap; 
	
	static
	{
		classMap = new HashMap<String, String>();
		classMap.put(SelectQuery.class.getCanonicalName(), 
						NonBlockingSelectQuery.class.getCanonicalName());
		classMap.put(SelectWithin.class.getCanonicalName(), 
						NonBlockingSelectWithin.class.getCanonicalName());
		classMap.put(SelectNear.class.getCanonicalName(), 
						NonBlockingSelectNear.class.getCanonicalName());
		classMap.put(SelectGroupSetupQuery.class.getCanonicalName(), 
				NonBlockingSelectGroupSetupQuery.class.getCanonicalName());
		classMap.put(SelectGroupSetupQueryWithGuid.class.getCanonicalName(), 
				NonBlockingSelectGroupSetupQuery.class.getCanonicalName());
		classMap.put(SelectGroupSetupQueryWithGuidAndInterval.class.getCanonicalName(), 
				NonBlockingSelectGroupSetupQuery.class.getCanonicalName());
	}
	
	protected final CommandPacket wosignCmd;
	protected final AbstractCommand blockingSelectCmd;
	
	public AbstractNonBlockingSelectCmd(CommandPacket wosignCmd, AbstractCommand blockingSelectCmd)
	{
		this.wosignCmd = wosignCmd;
		this.blockingSelectCmd = blockingSelectCmd;
	}
	
	/**
	 * Returns the custom select request packet based on each type of select 
	 * in the select request. Refer edu.umass.cs.gnsserver.gnsapp.packet.SelectOperation
	 * @return
	 */
	public abstract SelectRequestPacket getSelectRequestPacket(JSONObject commandJSON) throws JSONException;
	
	
	/**
	 * Creates the object of clazz by reflection. clazz will be a child class of
	 * AbstractSelect whose class path will be specified in the GNS config files.
	 * @param clazz
	 * @return
	 */
	public static AbstractNonBlockingSelectCmd getNonBlockingCmd
											(CommandPacket wosignCmd, AbstractCommand blockingSelectCmd) 
	{
		String blockingClass = blockingSelectCmd.getClass().getCanonicalName();	
		
		if(classMap.containsKey(blockingClass))
		{
			String nonBlockingClassName = classMap.get(blockingClass);
			Class<?> nonBlockingClass = null;
			try
			{
				nonBlockingClass = Class.forName(nonBlockingClassName);
				return (AbstractNonBlockingSelectCmd) 
					nonBlockingClass.getConstructor(CommandPacket.class, AbstractCommand.class).newInstance
							(wosignCmd, blockingSelectCmd);
			}
			catch (ClassNotFoundException  | InstantiationException | IllegalAccessException
					| IllegalArgumentException | InvocationTargetException
					| NoSuchMethodException | SecurityException e)
			{
				throw new RuntimeException("Unable to find or create object of class "
							+nonBlockingClassName +" exception "+e.getMessage());
			}
		}
		else
		{
			throw new RuntimeException("Unable to find a non blocking class for a "
					+ "blocking select request class "+blockingClass);
		}
	}
}