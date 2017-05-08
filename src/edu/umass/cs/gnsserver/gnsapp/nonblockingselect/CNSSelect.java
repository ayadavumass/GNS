package edu.umass.cs.gnsserver.gnsapp.nonblockingselect;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.packets.PacketUtils;
import edu.umass.cs.gnscommon.packets.ResponsePacket;
import edu.umass.cs.gnsserver.gnsapp.GNSApp;
import edu.umass.cs.gnsserver.gnsapp.NSSelectInfo;
import edu.umass.cs.gnsserver.gnsapp.Select;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandHandler;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.FieldAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.gnsapp.nonblockingselect.commands.AbstractNonBlockingSelectCmd;
import edu.umass.cs.gnsserver.gnsapp.nonblockingselect.helper.PendingSelectReqInfo;
import edu.umass.cs.gnsserver.gnsapp.packet.BasicPacketWithClientAddress;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectRequestPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectResponsePacket;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;

/**
 * This class implements the CNS select mechanism.
 * The set of nodes that a select request contacts is based 
 * on the demand profile and select policy used. 
 * 
 * @author ayadav
 */
public class CNSSelect extends AbstractSelect
{
	private final ConcurrentHashMap<Integer, PendingSelectReqInfo> pendingSelectRequests;
	
	private final Object selectLock				= new Object();
	private final Random rand					= new Random();
	
	public CNSSelect(GNSApp gnsApp) 
	{
		super(gnsApp);
		pendingSelectRequests 
				= new ConcurrentHashMap<Integer, PendingSelectReqInfo>();
	}
	
	@Override
	public void handleSelectRequestFromClient(CommandPacket request)
	{
		System.out.println("CNSSelect: handleSelectRequestFromClient "+request);
		
		try 
		{
			// from now on we only use this as command packet, and not the request
			CommandPacket wosignCmd = CommandHandler.addMessageWithoutSignatureToCommand(request);
			
			AbstractCommand blockingSelectCmd = 
								CommandHandler.commandModule.lookupCommand
											(PacketUtils.getCommand(request));
			
			//TODO: need for group lookup and creation, which is not implemented yet.
//			InternalRequestHeader interReqHeader 
//							= CommandHandler.getInternalHeaderAfterEnforcingChecks(wosignCmd);
			
			AbstractNonBlockingSelectCmd nonblockingCmd = 
							AbstractNonBlockingSelectCmd.getNonBlockingCmd
													(wosignCmd, blockingSelectCmd);
			
			JSONObject commandJSON = wosignCmd.getCommand();
			
			SelectRequestPacket selectPacket = nonblockingCmd.getSelectRequestPacket(commandJSON);
			
			if(selectPacket == null)
			{
				//FIXME: send an error back to client
			}
			else
			{
				// signature check for select
				String reader = commandJSON.optString(GNSProtocol.GUID.toString(), null);
				String signature = commandJSON.optString(GNSProtocol.SIGNATURE.toString(), null);
			    String message = commandJSON.optString
			    				(GNSProtocol.SIGNATUREFULLMESSAGE.toString(), null);
			    
				if (!FieldAccess.signatureCheckForSelect(reader, signature, message, gnsApp)) 
				{
					//return null;
					// FIXME: send bad signature message to client
				}
				else
				{
					Set<InetSocketAddress> serverAddresses 
								= gnsApp.getSelectPolicy().getNodesForSelectRequest(selectPacket);
					
					System.out.println("serverAddresses "+serverAddresses);
					
					int requestId = addSelectRequestIntoPendingMap(selectPacket, 
													serverAddresses, wosignCmd);
					
					InetSocketAddress returnAddress 
							= new InetSocketAddress(gnsApp.getNodeAddress().getAddress(),
				            ReconfigurationConfig.getClientFacingPort(gnsApp.getNodeAddress().getPort()));
					
					selectPacket.setNSReturnAddress(returnAddress);
					selectPacket.setNsQueryId(requestId); // Note: this also tells handleSelectRequest that it should go to NS now
				    JSONObject outgoingJSON = selectPacket.toJSONObject();
				    
				    for (InetSocketAddress address : serverAddresses)
				    {
				    	InetSocketAddress offsetAddress = new InetSocketAddress(address.getAddress(),
				                  ReconfigurationConfig.getClientFacingPort(address.getPort()));
				    	
				    	LOG.log(Level.INFO, "NS {0} sending select {1} to {2} ({3})",
				                  new Object[]{gnsApp.getNodeID(), outgoingJSON, offsetAddress, address});
				    	gnsApp.sendToAddress(offsetAddress, outgoingJSON);
				    }
				}
			}
			
		}
		catch(JSONException jsonex)	
		{
			//FIXME: send error message to client
			jsonex.printStackTrace();
		} catch (IOException e) 
		{
			//FIXME: send error message to client
			e.printStackTrace();
		}
	}
	
	
	@Override
	public void handleSelectRequestAtNS(SelectRequestPacket selectReq)
	{
		// original Select.handleSelectRequest is used 
	}
	
	
	@Override
	public void handleSelectResponseFromNS(SelectResponsePacket selectResp) throws IOException, JSONException
	{
		System.out.println("handleSelectResponseFromNS "+selectResp);
		LOG.log(Level.FINE,
				"NS {0} recvd from NS {1}",
				new Object[]{gnsApp.getNodeID(),
						selectResp.getNSAddress()});
		
		PendingSelectReqInfo pendingSelect = pendingSelectRequests.get(selectResp.getNsQueryId());
		
	    
	    if (pendingSelect == null)
	    {
	    	LOG.log(Level.WARNING,
	    			"NS {0} unabled to located query info:{1}",
	    			new Object[]{gnsApp.getNodeID(), selectResp.getNsQueryId()});
	    	//FIXME: Need to check if this case ever occurs. 
	    	return;
	    }
	    
	    // if there is no error update our results list
	    if (SelectResponsePacket.ResponseCode.NOERROR.equals(selectResp.getResponseCode())) 
	    {
	    	// stuff all the unique records into the info structure
	    	try 
	    	{
	    		Select.processJSONRecords(selectResp.getRecords(), pendingSelect.getNSInfo(), gnsApp);
			} 
	    	catch (JSONException e) 
	    	{
				e.printStackTrace();
				//FIXME: need to add code here to indicate to the client 
				// that the search reply doesn't have all GUIDs.
			}
	    }
	    else 
	    {
	      // error response
	      LOG.log(Level.FINE,
	    		  "NS {0} processing error response: {1}",
	              new Object[]{gnsApp.getNodeID(), selectResp.getErrorMessage()});
	    }
	    // Remove the NS Address from the list to keep track of who has responded
	    boolean allServersResponded;
	    /* synchronization needed, otherwise assertion in app.sendToClient
	     * implying that an outstanding request is always found gets violated. */
	    synchronized (pendingSelect.getNSInfo()) 
	    {
	    	// Remove the NS Address from the list to keep track of who has responded
	    	pendingSelect.getNSInfo().removeServerAddress(selectResp.getNSAddress());
	    	allServersResponded = pendingSelect.getNSInfo().allServersResponded();
	    }
	    if (allServersResponded)
	    {
	    	handledAllServersResponded(selectResp, pendingSelect);
	    }
	    else 
	    {
	    	LOG.log(Level.FINE,
	    			"NS{0} servers yet to respond:{1}",
	    			new Object[]{gnsApp.getNodeID(), 
	    					pendingSelect.getNSInfo().serversYetToRespond()});
	    }
	}
	
	
	private void handledAllServersResponded(
	          SelectResponsePacket incomingSelectResp, PendingSelectReqInfo pendingSelect) 
	        		  								throws IOException, JSONException
	{
		System.out.println("handledAllServersResponded ");
		// must be done before the notify below
	    // we're done processing this select query
		
		Set<JSONObject> allRecords = pendingSelect.getNSInfo().getResponsesAsSet();
	    // Todo - clean up this use of guids further below in the group code
	    Set<String> guids = Select.extractGuidsFromRecords(allRecords);
	    
	    LOG.log(Level.FINE, 
	    		"NS{0} guids:{1}", new Object[]{gnsApp.getNodeID(), guids});
	    
	    SelectResponsePacket responseToClient;
	    // If projection is null we return guids (old-style).
	    if (pendingSelect.getNSInfo().getProjection() == null) 
	    {
	    	responseToClient = SelectResponsePacket.makeSuccessPacketForGuidsOnly(
	    			incomingSelectResp.getId(), null, -1, null, new JSONArray(guids));
	    	// Otherwise we return a list of records.
	    }
	    else
	    {
	    	List<JSONObject> records = Select.filterAndMassageRecords(allRecords);
	    	LOG.log(Level.FINE, "NS{0} record:{1}",
	              new Object[]{gnsApp.getNodeID(), records});
	    	
	    	responseToClient = SelectResponsePacket.makeSuccessPacketForFullRecords
	    			(incomingSelectResp.getId(), null, -1, -1, null, new JSONArray(records));
	    }
	    
	    //FIXME: projection code missing here.
	    JSONArray resultGUIDs = responseToClient.getGuids();
	    
	    CommandResponse clientResp = null;
	    
	    if(resultGUIDs != null)
	    {
	    	clientResp = new CommandResponse(ResponseCode.NO_ERROR, resultGUIDs.toString());
	    }
	    else
	    {
	    	// FIXME: sending empty JSON for now. Need to check what is the right response.
	    	clientResp = new CommandResponse(ResponseCode.NO_ERROR, new JSONArray().toString());
	    }
	    
	    //FIXME:  the last arguments here in the call below are instrumentation
	    // that the client can use to determine LNS load
	    ResponsePacket returnPacket = new ResponsePacket(
	    		pendingSelect.getOriginalRequest().getRequestID(),
	    		pendingSelect.getOriginalRequest().getServiceName(), clientResp, 0, 0,
	              System.currentTimeMillis() - pendingSelect.getReqArrivalTime());
	    
	    // remove before sending response
	    this.pendingSelectRequests.remove(pendingSelect.getNSInfo().getId());
	    
	    System.out.println("Client response original request "+pendingSelect.getOriginalRequest()
	    			+" returnPacket "+returnPacket);
	    
	    System.out.println("Client response returnPacket "+returnPacket);
	    
	    //Note: gnsApp.sendToClient won't work here because the gnsApp.execute
	    // method called by gigapxos already returned, so it won't send any
	    // delegated messages.  
	    // We need to directly send the message to the client. No delegation allowed.
	    //gnsApp.sendToClient(pendingSelect.getOriginalRequest(), 
		//		returnPacket, returnPacket.toJSONObject());
	    
	    InetSocketAddress clientAddress = ((BasicPacketWithClientAddress) 
	    						pendingSelect.getOriginalRequest()).getClientAddress();
	    System.out.println("\n\n clientAddress "+clientAddress+"\n\n");
	    
	    clientAddress = ((ReplicableClientRequest)pendingSelect.getOriginalRequest()).getClientAddress();
	    
	    	
	    
	    gnsApp.sendToAddress(clientAddress, returnPacket.toJSONObject());
	}
	
	
	private int addSelectRequestIntoPendingMap(SelectRequestPacket selectPacket, 
												Set<InetSocketAddress> serverAddresses,
												CommandPacket wosignCmd)
	{
		int reqId = -1;
		synchronized(this.selectLock)
		{
			do
			{
				reqId = rand.nextInt();
			} while (pendingSelectRequests.containsKey(reqId));
			
			//Add query info
			NSSelectInfo info = new NSSelectInfo(reqId, serverAddresses, 
						selectPacket.getSelectOperation(), 
						selectPacket.getGroupBehavior(),
						selectPacket.getQuery(),
						selectPacket.getProjection(),
						selectPacket.getMinRefreshInterval(), selectPacket.getGuid());
			
			PendingSelectReqInfo selectInfo 
							= new PendingSelectReqInfo
							(wosignCmd, info);
			
			
			pendingSelectRequests.put(reqId, selectInfo);
		}
		return reqId;
	}
}