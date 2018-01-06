package edu.umass.cs.capacitytesting;

import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.capacitytesting.AbstractRequestSender;
import edu.umass.cs.gigapaxos.interfaces.Callback;

/**
 * Init call back for GUID creation.
 * @author ayadav
 *
 */
public class InitCallBack implements Callback<CommandPacket, CommandPacket>
{	
	private final AbstractRequestSender sendingObj;
	private final long startTime;
	private final int reqProbeNum;
	
	public InitCallBack(int reqProbeNum, AbstractRequestSender sendingObj)
	{
		this.sendingObj = sendingObj;
		startTime = System.currentTimeMillis();
		this.reqProbeNum = reqProbeNum;
	}
	
	@Override
	public CommandPacket processResponse(CommandPacket request) 
	{
		CommandPacket cmd = (CommandPacket) request;
		
		System.out.println("Summary " + cmd.getResponse().getSummary());
		
		sendingObj.incrementUpdateNumRecvd(reqProbeNum, cmd.getServiceName(), 
				(System.currentTimeMillis()-startTime));
		return cmd;
	}
}