package edu.umass.cs.gnsserver.gnsapp.nonblockingselect.commands;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectRequestPacket;

public class NonBlockingSelectGroupLookupQuery extends AbstractNonBlockingSelectCmd
{
	
	public NonBlockingSelectGroupLookupQuery
					(CommandPacket wosignCmd, AbstractCommand blockingSelectCmd) {
		super(wosignCmd, blockingSelectCmd);
	}

	@Override
	public SelectRequestPacket getSelectRequestPacket(JSONObject commandJSON) 
									throws JSONException 
	{
		return null;
	}
	
}