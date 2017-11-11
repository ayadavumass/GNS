package edu.umass.cs.gnsserver.gnsapp.nonblockingselect;


import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;

import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.GNSApp;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectRequestPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectResponsePacket;

/**
 * This class defines the methods that a select request processing class
 * should implement. This class replaces 
 * edu.umass.cs.gnsserver.gnsapp.Select class in the GNS.
 * The GNS takes the select request processing classpath as
 * input from config files and creates an object using reflection. Ideally, the 
 * select processing class would be included in the CNS project. 
 * 
 * @author ayadav
 */
public abstract class AbstractSelect 
{	
	protected static final Logger LOG = Logger.getLogger(AbstractSelect.class.getName());
	
	protected final GNSApp gnsApp;
	
	
	public AbstractSelect(GNSApp gnsApp)
	{
		this.gnsApp = gnsApp;
	}
	
	/**
	 * This method processes a select request from a client in a non-blocking manner.
	 * 
	 * One thing to note is that this method doesn't need to be blocking. 
	 * The code chain for this method is as follows. The gigapaxos calls GNSApp.execute(.), in 
	 * the COMMAND case, if the command is a select command then we call this method. 
	 * If the command is not a select command then we process as usual in the GNAApp.execute(.).
	 * The  GNSApp.execute(.) method doesn't need to block for the completion of a
	 * SelectRequest, i.e., GNSApp.execute calls handleSelectRequestFromClient method and 
	 * this method forwards the select request to the  required NSs and returns. 
	 * @param request
	 * @return Returns {@link SelectFuture}, which the caller of handleSelectRequestFromClient can use 
	 * to wait for the completion. 
	 */
	public abstract SelectFuture handleSelectRequestFromClient(CommandPacket request) throws JSONException;
	
	
	/**
	 * This method is called in the SELECT_REQUEST case of GNAApp.execute(.)
	 * This method is called on NSs that process a select request and return GUIDs  
	 * to the select request originating NS. 
	 * 
	 * This method performs DB operations but it is not blocking. For improving the 
	 * performance we can perform DB operations in a separate thread pool, so that
	 * the gigapaxos or NIO threads, which are small in number, don't block for
	 * DB operations. One TODO: is to check if mongodb allows callback based DB operations. 
	 * 
	 * @param selectReq
	 */
	public abstract void handleSelectRequestAtNS(SelectRequestPacket selectReq);
	
	
	/**
	 * This method is called at a select request originating NS when another NS
	 * replies back for an earlier select request. 
	 * In this method, we aggregate the response GUIDs and if all the contacted NSs have 
	 * responded then we send a reply back to the client. 
	 * 
	 * @param selectResp
	 */
	public abstract void handleSelectResponseFromNS(SelectResponsePacket selectResp) 
																	throws IOException, JSONException;
	
	/**
	 * Creates the object of clazz by reflection. clazz will be a child class of
	 * AbstractSelect whose class path will be specified in the GNS config files.
	 * @param clazz
	 * @return
	 */
	public static AbstractSelect createSelectObject(Class<?> clazz, GNSApp gnsApp) 
	{
		try 
		{
			return (AbstractSelect) clazz.getConstructor(GNSApp.class).newInstance(gnsApp);
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			LOG.log(Level.SEVERE, 
					e.getClass().getSimpleName() + " while creating " + clazz);
			e.printStackTrace();
		}
		return null;
	}
}