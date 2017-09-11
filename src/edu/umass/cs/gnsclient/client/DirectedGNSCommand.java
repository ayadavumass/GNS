package edu.umass.cs.gnsclient.client;

import java.net.InetSocketAddress;

import org.json.JSONObject;

/**
 * A directed GNSCommand that goes to a specific name server. 
 * @author ayadav
 *
 */
public class DirectedGNSCommand extends GNSCommand
{
	private final InetSocketAddress nameServerAddress;
	
	/* DirectedGNSCommand constructors must remain private */
	  /**
	   *
	   * @param command
	   */
	  protected DirectedGNSCommand(InetSocketAddress nameServerAddress, 
			  		JSONObject command) 
	  {
		  this(nameServerAddress,
	            /**
	             * Generate a random value here because it is not easy (or
	             * worth trying) to guarantee non-conflicting IDs here. Conflicts will
	             * either result in an IOException further down or the query will be
	             * transformed to carry a different ID if
	             */
	            randomLong(), command);
	  }

	  /**
	   *
	   * @param id
	   * @param command
	   */
	  protected DirectedGNSCommand(InetSocketAddress nameServerAddress,
			  long id, JSONObject command) 
	  {
		  super(id, command);
		  this.nameServerAddress = nameServerAddress;
	  }
	  
	  public InetSocketAddress getNameServerAddress()
	  {
		  return this.nameServerAddress;
	  }
}