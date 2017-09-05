package edu.umass.cs.gnscommon.packets;

import java.net.InetSocketAddress;

/**
 * A directed version of CommandPacket. In a directed version,
 * a command packet contains the server address where the gigapaxos 
 * should forward the command packet.
 * 
 * @author ayadav
 *
 */
public class DirectedCommandPacket extends CommandPacket
{
	public DirectedCommandPacket(CommandPacket cmd, InetSocketAddress socketAddress)
	{
		super(cmd.getRequestID(), cmd.getCommand(), socketAddress);
	}
}
