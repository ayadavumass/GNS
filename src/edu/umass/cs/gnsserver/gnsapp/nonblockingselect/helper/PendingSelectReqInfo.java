package edu.umass.cs.gnsserver.gnsapp.nonblockingselect.helper;

import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.NSSelectInfo;

/**
 * Stores the information about pending select requests.
 * @author ayadav
 *
 */
public class PendingSelectReqInfo
{
	private final CommandPacket originalRequest;
	private final long reqArrivalTime;
	private final NSSelectInfo nsInfo;
	
	public PendingSelectReqInfo(CommandPacket originalRequest, NSSelectInfo nsInfo)
	{
		this.originalRequest = originalRequest;
		this.reqArrivalTime = System.currentTimeMillis();
		this.nsInfo = nsInfo;
	}
	
	public CommandPacket getOriginalRequest()
	{
		return this.originalRequest;
	}
	
	public long getReqArrivalTime()
	{
		return this.reqArrivalTime;
	}
	
	public NSSelectInfo getNSInfo()
	{
		return this.nsInfo;
	}
}