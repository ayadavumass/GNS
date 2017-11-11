package edu.umass.cs.gnsserver.gnsapp.nonblockingselect.commands;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectRequestPacket;

/**
 * This class is non-blocking version of the following classes in 
 * edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select package
 * 1) SelectQuery
 * 2) SelectNear
 * 3) SelectWithin
 * 
 * @author ayadav
 */
public class NonBlockingSelectWithin extends AbstractNonBlockingSelectCmd
{
	
	public NonBlockingSelectWithin(CommandPacket wosignCmd, AbstractCommand blockingSelectCmd) 
	{
		super(wosignCmd, blockingSelectCmd);
	}

	@Override
	public SelectRequestPacket getSelectRequestPacket(JSONObject commandJSON) throws JSONException 
	{
		return null;
	}
}