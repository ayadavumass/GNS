package edu.umass.cs.gnsserver.gnsapp.nonblockingselect.commands;

import java.util.ArrayList;
import java.util.Arrays;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.Select;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectRequestPacket;
import edu.umass.cs.gnsserver.utils.JSONUtils;

/**
 * This class is non-blocking version of the following classes in 
 * edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select package
 * 1) SelectQuery
 * 2) SelectNear
 * 3) SelectWithin
 * 
 * @author ayadav
 */
public class NonBlockingSelectQuery extends AbstractNonBlockingSelectCmd
{

	public NonBlockingSelectQuery(CommandPacket wosignCmd, AbstractCommand blockingSelectCmd) 
	{
		super(wosignCmd, blockingSelectCmd);	
	}

	@Override
	public SelectRequestPacket getSelectRequestPacket(JSONObject commandJSON) throws JSONException
	{	
		String reader = commandJSON.optString(GNSProtocol.GUID.toString(), null);
		String query = commandJSON.getString(GNSProtocol.QUERY.toString());
		
		if (Select.queryContainsEvil(query)) 
		{
			return null;
		}
		
	    // Special case handling of the fields argument
	    // Empty means this is an older style select which is converted to null
	    // GNSProtocol.ENTIRE_RECORD means what you think and is converted to a unique value which
	    // is the fields array is length one and has GNSProtocol.ENTIRE_RECORD string as the first element
	    // otherwise it is a list of fields
	    ArrayList<String> projection;
	    if (!commandJSON.has(GNSProtocol.FIELDS.toString())) 
	    {
	    	projection = null;
	    } else if (GNSProtocol.ENTIRE_RECORD.toString().equals(commandJSON.optString
	    													(GNSProtocol.FIELDS.toString()))) 
	    {
	    	projection = new ArrayList<>(Arrays.asList(GNSProtocol.ENTIRE_RECORD.toString()));
	    } else 
	    {
	    	projection = JSONUtils.JSONArrayToArrayListString(commandJSON.getJSONArray
	    												(GNSProtocol.FIELDS.toString()));
	    }
	    
		SelectRequestPacket packet = SelectRequestPacket.MakeQueryRequest(-1, reader, query, projection);
		
		return packet;
	}
}