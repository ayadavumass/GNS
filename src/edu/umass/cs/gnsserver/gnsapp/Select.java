/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.gnsapp;

/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved
 */
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnscommon.packets.PacketUtils;
import edu.umass.cs.gnscommon.packets.commandreply.NotificationStatsToIssuer;
import edu.umass.cs.gnscommon.packets.commandreply.SelectHandleInfo;
import edu.umass.cs.gnsserver.database.AbstractRecordCursor;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.InternalField;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.MetaDataTypeName;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSAuthentication;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectOperation;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectRequestPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectResponsePacket;
import edu.umass.cs.gnsserver.gnsapp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.gnsapp.selectnotification.EntryPointSelectNotificationState;
import edu.umass.cs.gnsserver.gnsapp.selectnotification.NotificationSendingStats;
import edu.umass.cs.gnsserver.gnsapp.selectnotification.PendingSelectNotifications;
import edu.umass.cs.gnsserver.gnsapp.selectnotification.SelectGUIDInfo;
import edu.umass.cs.gnsserver.gnsapp.selectnotification.SelectResponseProcessor;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.main.GNSConfig.GNSC;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.utils.Config;

/**
 * This class handles select operations which have a similar semantics to an SQL SELECT.
 * The base architecture of the select methods is that we broadcast a query
 * to all the servers and collate the responses.
 * The most general purpose select method is implemented in SelectQuery. It takes a
 * mongo-style query and returns all the records that match that query.
 *
 * SelectQuery can be used two ways. The older style is that it returns a
 * list of GUIDS. The newer style is that it returns entire records or
 * subsets of the fields in records. The guid of the record is returned
 * using the special "_GUID" key.
 * The behavior of SelectQuery is controlled by the
 * field (or projection) parameter. In this code if it is null that means
 * old style.
 *
 * Select, SelectNear, SelectWithin all handle specific types of queries.
 * They remove the need for the user to understand mongo syntax, but are really not necessary.
 *
 *
 * The idea is that we want to look up all the records with a given value or whose
 * value falls in a given range or that more generally match a query.
 *
 * For all select operations the NS which receive the broadcasted select packet execute the
 * appropriate query to collect all the guids that satisfy it. They then send the full records
 * from all these queries back to the collecting NS. The collecting NS then extracts the GUIDS
 * from all the results removing duplicates and then sends back JUST THE GUIDs, not the full
 * records.
 *  *
 * @author westy
 */
public class Select extends AbstractSelector 
{	
	protected SelectResponseProcessor notificationSender;
	
	// for entry-point name server
	private final EntryPointSelectNotificationState entryPointState;
	
	// for any name server.
	private final PendingSelectNotifications pendingNotifications;
	
	
	private static final Random RANDOM_ID = new Random();
	
	private static final ConcurrentMap<Integer, NSSelectInfo> QUERIES_IN_PROGRESS
          = new ConcurrentHashMap<>(10, 0.75f, 3);
	private static final ConcurrentMap<Integer, SelectResponsePacket> QUERY_RESULT
          = new ConcurrentHashMap<>(10, 0.75f, 3);
	
	public Select()
	{
		initSelectResponseProcessor();
		entryPointState = new EntryPointSelectNotificationState();
		
		pendingNotifications = new PendingSelectNotifications();
	}
	
  
	public void initSelectResponseProcessor()
	{
		Class<?> clazz = null;
		try 
		{
			clazz = (Class.forName(Config
					.getGlobalString(GNSConfig.GNSC.SELECT_REPONSE_PROCESSOR)));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	  
		if (clazz != null)
		{
			try 
			{
				notificationSender = (SelectResponseProcessor) (clazz.getConstructor().newInstance());
			} catch (InstantiationException | IllegalAccessException
					| IllegalArgumentException | InvocationTargetException
					| NoSuchMethodException | SecurityException e) 
			{
				LOGGER.log(Level.WARNING,
					  "{0} unable to instantiate select response processor {1}.",
								new Object[] {
										GNSConfig.class.getName(),
										Config.getGlobalString(GNSConfig.GNSC.SELECT_REPONSE_PROCESSOR) });
				e.printStackTrace();
			}
		}
	}
  
  /**
   * Handles a select request that was received from a client.
   *
   * @param packet
   *
   * @param replica
   * @throws JSONException
   * @throws UnknownHostException
   * @throws FailedDBOperationException
   */
  @Override
  public void handleSelectRequest(SelectRequestPacket packet,
          GNSApplicationInterface<String> replica) throws JSONException, UnknownHostException, FailedDBOperationException {
    if (packet.getNsQueryId() != -1) { // this is how we tell if it has been processed by the NS
      handleSelectRequestFromNS(packet, replica);
    } else {
      throw new UnsupportedOperationException("SelectRequestPacket from client should not be coming here.");
    }
  }

  //FIXME: We need to determine this timeout systematically, not an ad hoc constant.
  private static final long SELECT_REQUEST_TIMEOUT = Config.getGlobalInt(GNSConfig.GNSC.SELECT_REQUEST_TIMEOUT);

  /**
   * Handle a select request from a client.
   * This node is the broadcaster and selector.
   *
   * @param header
   * @param packet
   * @param app
   * @return a select response packet
   * @throws JSONException
   * @throws UnknownHostException
   * @throws FailedDBOperationException
   * @throws InternalRequestException
   */
//  @Override
  public  SelectResponsePacket handleSelectRequestFromClient(InternalRequestHeader header,
          SelectRequestPacket packet,
          GNSApplicationInterface<String> app) throws JSONException, UnknownHostException,
          FailedDBOperationException, InternalRequestException 
  {
	  switch(packet.getSelectOperation())
	  {
	  		case EQUALS:
	  		case NEAR:
	  		case WITHIN:
	  		case QUERY:
	  		{
	  			return processSelectRequestFromClient();
	  			//break;
	  		}
	  		case SELECT_NOTIFY:
	  		{
	  			return processSelectRequestFromClient();
	  			//break;
	  		}
	  		case NOTIFICATION_STATUS:
	  		{
	  			return processNotificationStatusFromClient();
	  			//break;
	  		}
	  		default:
	  			break;
	  }
	  //TODO: The code needs to be moved to the appropriate function.
	  
	  Set<InetSocketAddress> serverAddresses = new HashSet<>(PaxosConfig.getActives().values());
	  
	  // store the info for later
	  int queryId = addQueryInfo(serverAddresses, packet.getSelectOperation(),
            packet.getQuery(), packet.getProjection());
	  
	  InetSocketAddress returnAddress = new InetSocketAddress(app.getNodeAddress().getAddress(),
            ReconfigurationConfig.getClientFacingPort(app.getNodeAddress().getPort()));
	  packet.setNSReturnAddress(returnAddress);
	  //packet.setNameServerID(app.getNodeID());
	  packet.setNsQueryId(queryId); // Note: this also tells handleSelectRequest that it should go to NS now
	  JSONObject outgoingJSON = packet.toJSONObject();
	  try 
	  {
		  LOGGER.log(Level.FINER, "addresses: {0} node address: {1}",
              new Object[]{serverAddresses, app.getNodeAddress()});
		  // Forward to all but self because...
		  for (InetSocketAddress address : serverAddresses) 
		  {
			  InetSocketAddress offsetAddress = new InetSocketAddress(address.getAddress(),
                  ReconfigurationConfig.getClientFacingPort(address.getPort()));
			  LOGGER.log(Level.INFO, "NS {0} sending select {1} to {2} ({3})",
                  new Object[]{app.getNodeID(), outgoingJSON, offsetAddress, address});
			  app.sendToAddress(offsetAddress, outgoingJSON);
		  }
      
		  // Wait for responses, otherwise you are violating Replicable.execute(.)'s semantics.
		  synchronized (QUERIES_IN_PROGRESS) 
		  {
			  while (QUERIES_IN_PROGRESS.containsKey(queryId)) 
			  {
				  try 
				  {
					  QUERIES_IN_PROGRESS.wait(SELECT_REQUEST_TIMEOUT);
				  } catch (InterruptedException e) {
					  e.printStackTrace();
				  }
			  }
		  }
		  if (QUERY_RESULT.containsKey(queryId)) 
		  {
			  return QUERY_RESULT.remove(queryId);
		  }
	  } catch (IOException  e) 
	  {
		  LOGGER.log(Level.SEVERE, "Exception while sending select request: {0}", e);
	  }
	  return null;
  }
  
  private SelectResponsePacket processSelectRequest(
		  SelectRequestPacket request, GNSApplicationInterface<String> app) 
				  	throws FailedDBOperationException, JSONException
  {
	  AbstractRecordCursor cursor = getDBCursor(request, app);
	  
	  switch (request.getSelectOperation())
	  {
	  		case EQUALS:
		  	case NEAR:
		    case WITHIN:
		    case QUERY:
		    {
		    	// aditya:TODO: These cases also needs to be changed to a distributed
		    	// iterator approach. 
		    	JSONArray resultRecords = new JSONArray();
		    	
				while (cursor != null && cursor.hasNext()) 
				{
					JSONObject record = cursor.nextJSONObject();
					
					record = aclCheckForRecord(request, record, app);
					if(record!=null)
					{
						record = performProjectionForUserRequestedAttributes(
							  app, request, record);
						
						if(record!=null)
							resultRecords.put(record);
					}
				}
				
				return SelectResponsePacket.makeSuccessPacketForFullRecords(
						request.getId(), request.getClientAddress(),
						request.getNsQueryId(), app.getNodeAddress(), resultRecords);
		    }
		    case SELECT_NOTIFY:
		    {
		    	LOGGER.log(Level.FINE, "NS{0} query: {1} {2}",
		                new Object[]{app.getNodeID(), request.getQuery(), request.getProjection()});
		    	
		    	String notificationStr = request.getNotificationString();
		    	
		    	List<SelectGUIDInfo> currList = new LinkedList<SelectGUIDInfo>();
		    	long localhandle = -1;
		    	while (cursor != null && cursor.hasNext()) 
				{
					JSONObject record = cursor.nextJSONObject();
					
					record = aclCheckForRecord(request, record, app);
					if(record != null)
					{
						record = performProjectionForUserRequestedAttributes(
							  app, request, record);
						
						if(record!=null)
						{
							try 
							{
								String guid = record.getString(NameRecord.NAME.getName());
								SelectGUIDInfo selectGUIDInfo = new SelectGUIDInfo(guid, record);
								currList.add(selectGUIDInfo);
							} catch (JSONException e) 
							{
								// This JSON exception is because of problem in reading NameRecord.NAME.
								// which should not happen
								LOGGER.log(Level.INFO, "JSONException in processing a record {0}", 
										new Object[]{e.getMessage()});
							}
						}
					}
					
					if(currList.size() >= Config.getGlobalInt(GNSC.SELECT_FETCH_SIZE))
					{
						if(localhandle == -1)
						{
							localhandle = this.pendingNotifications.getUniqueIDAndInit();
						}
						
						// Sending the actual notification.
						// Based on the implementation, this function could block for very long.
						// Ideally, the implementation of this function should not be blocking. 
						// The design is such that notification function can update the progress
						// of notification sending using InternalNotificationStats, and the GNS
						// can get the progress using NotificationSendingStats.
						NotificationSendingStats stats 
								= this.notificationSender.sendNotification(currList, notificationStr);
						
						this.pendingNotifications.addNotificationStats(localhandle, stats);
						
						// Not clearing currList here, as the notification function 
						// may be using it.
						// So just re-initializing it. 
						currList = new LinkedList<SelectGUIDInfo>();
					}
				}
		    	
		    	// last batch.
		    	if(currList.size() > 0)
		    	{
		    		if(localhandle == -1)
					{
						localhandle = this.pendingNotifications.getUniqueIDAndInit();
					}
					
					// Sending the actual notification.
					// Based on the implementation, this function could block for very long.
					// Ideally, the implementation of this function should not be blocking. 
					// The design is such that notification function can update the progress
					// of notification sending using InternalNotificationStats, and the GNS
					// can get the progress using NotificationSendingStats.
					NotificationSendingStats stats 
							= this.notificationSender.sendNotification(currList, notificationStr);
					
					this.pendingNotifications.addNotificationStats(localhandle, stats);
		    	}
		    	
		    	List<NotificationSendingStats> allStats = this.pendingNotifications.lookupNotificationStats(localhandle);
		    	long totalNot = 0;
		    	long totalFailed = 0;
		    	long totalPending = 0;
		    	if(allStats != null)
		    	{
		    		for(int i=0; i<allStats.size(); i++)
		    		{
		    			totalNot+=allStats.get(i).getTotalNotifications();
		    			totalFailed+=allStats.get(i).getGUIDsFailed().size();
		    			totalPending+=allStats.get(i).getNumberPending();
		    		}
		    	}
		    	
		    	SelectHandleInfo selectHandle 
		    				= new SelectHandleInfo(localhandle, app.getNodeAddress());
		    	
		    	NotificationStatsToIssuer toIssuer 
		    			= new NotificationStatsToIssuer(selectHandle, totalNot, 
		    												totalFailed, totalPending);
		    	
		    	//clearing the state
		    	this.pendingNotifications.removeNotificationInfo(localhandle);
		    	
		    	return SelectResponsePacket.makeSuccessPacketForNotificationStatsOnly
		    			(request.getId(), request.getClientAddress(),
								request.getNsQueryId(), app.getNodeAddress(), toIssuer);
		    }
		    default:
		        break;
	  }
	  return null;
  }
  
  
  private AbstractRecordCursor getDBCursor(SelectRequestPacket request, 
		  											GNSApplicationInterface<String> app) 
		  													throws FailedDBOperationException
  {
	  AbstractRecordCursor cursor = null;
	  switch (request.getSelectOperation()) 
	  {
	  		case EQUALS:
		  		cursor = NameRecord.selectRecords(app.getDB(), request.getKey(), request.getValue());
		  		break;
		  	case NEAR:
		  		if (request.getValue() instanceof String) {
		          cursor = NameRecord.selectRecordsNear(app.getDB(), request.getKey(), 
		        		  (String) request.getValue(), Double.parseDouble((String) request.getOtherValue()));
		        } else {
		          break;
		        }
		        break;
		    case WITHIN:
		        if (request.getValue() instanceof String) {
		          cursor = NameRecord.selectRecordsWithin(app.getDB(), request.getKey(), (String) request.getValue());
		        } else {
		          break;
		        }
		        break;
		    case QUERY:
		    case SELECT_NOTIFY:
		        LOGGER.log(Level.FINE, "NS{0} query: {1} {2}",
		                new Object[]{app.getNodeID(), request.getQuery(), request.getProjection()});
		        cursor = NameRecord.selectRecordsQuery(app.getDB(), request.getQuery(), 
		        														request.getProjection());
		        break;
		    default:
		        break;
	  }
	  return cursor;
  }

  /**
   * Handle a select request from the collecting NS. This is what other NSs do when they
   * get a SelectRequestPacket from the NS that originally received the packet (the one that is collecting
   * all the records).
   * This NS looks up the records and returns them.
   *
   * @param incomingJSON
   * @param app
   * @throws JSONException
   */
  private void handleSelectRequestFromNS(SelectRequestPacket request,
          GNSApplicationInterface<String> app) throws JSONException 
  {
	  LOGGER.log(Level.FINE,
            "NS {0} {1} received query {2}",
            new Object[]{Select.class.getSimpleName(),
              app.getNodeID(), request.getSummary()});
	  
	  switch(request.getSelectOperation())
	  {
		  case EQUALS:
		  case NEAR:
		  case WITHIN:
		  case QUERY:
		  {
			  processSelectRequestFromNSForReturningGUIDs();
			  break;
		  }
		  case SELECT_NOTIFY:
		  {
			  processSelectRequestFromNSForSelectNotify();
			  break;
		  }
		  case NOTIFICATION_STATUS:
		  {
			  processSelectRequestFromNSForNotificationStatus();
			  break;
		  }
		  default:
			  break;
	  }
	  
	  SelectResponsePacket response = null;
	  try 
	  {
		  response = processSelectRequest(request, app);
		  if(response == null)
		  {
			  response = SelectResponsePacket.makeFailPacket(request.getId(),
                    request.getClientAddress(),
                    request.getNsQueryId(), app.getNodeAddress(), "Unknown select operation");
		  }
	  } 
	  catch (FailedDBOperationException | JSONException e) 
	  {
		  LOGGER.log(Level.WARNING, "{0} exception while handling select request {1}: {2}"
    			, new Object[]{app, request.getSummary(), e});
		  response = SelectResponsePacket.makeFailPacket(request.getId(),
              request.getClientAddress(),
              request.getNsQueryId(), app.getNodeAddress(), e.getMessage());
	  }
	  
	  try 
	  {
		  app.sendToAddress(request.getNSReturnAddress(), response.toJSONObject());
	  } 
	  catch (IOException f) 
	  {
		  LOGGER.log(Level.SEVERE, "Unable to send Failure SelectResponsePacket: {0}", f);
	  }
  }
  
  
  /**
   * Checks if {@code record} satisfies ACL checks. Also, removes fields from {@code record} 
   * that the query issuer is not allowed to read. 
   *
   * @param packet
   * @param records
   * @param app
   * @return
   * The JSONObject corresponding to the record after removing private fields. Returns null
   * if ACL check on query fields fails or there is an exception.
   */
  private JSONObject aclCheckForRecord(SelectRequestPacket packet, JSONObject record,
		  GNSApplicationInterface<String> app) 
  {
	  // First we check if the query issuer is in read ACLs for all query attributes
	  boolean satisfy =aclCheckForQueryAttributes(packet, record, app);
	  if(satisfy)
	  {
		  return aclCheckForProjectionFields(packet, record, app);
	  }
	  else
	  {
		  return null;
	  }
  }
  
  /**
   * This function removes all the internal fields and fields that are not requested by the user.
   * This is needed because for signature and ACL checks we read internal fields too, so after all those 
   * checks are done, we remove internal fields and return only user requested fields.
   * @param app
   * @param packet
   * @param record
   * @return
   * Records after removing fields or null if there is some error.
   */
  private JSONObject performProjectionForUserRequestedAttributes(
		  GNSApplicationInterface<String> app, SelectRequestPacket packet, JSONObject record)
  {
	  List<String> projection = packet.getProjection();
	  
	  if (projection == null
			  // this handles the special case of the user wanting all fields 
			  // in the projection
			  || (!projection.isEmpty()
					  && projection.get(0).equals(GNSProtocol.ENTIRE_RECORD.toString())))
		  return record;


	  HashMap<String, Boolean> fieldsMap = new HashMap<String, Boolean>();
	  for(String field: projection)
	  {
		  fieldsMap.put(field, true);
	  }
	  
	  try {
		  // JSON iterator type warning suppressed.
		  @SuppressWarnings("unchecked")
		  Iterator<String> recordKeyIter = record.keys();
		  
		  while(recordKeyIter.hasNext())
		  {
			  String key = recordKeyIter.next();
			  if(key.equals(NameRecord.NAME.getName()))
			  {
				  // do nothing, as we want to keep the name field.
			  }
			  else if(key.equals(NameRecord.VALUES_MAP.getName()))
			  {
				  JSONObject valuesMap = record.getJSONObject(NameRecord.VALUES_MAP.getName());
				  // JSON iterator type warning suppressed.
				  @SuppressWarnings("unchecked")
				  Iterator<String> valueMapIter = valuesMap.keys();
				  while(valueMapIter.hasNext())
				  {
					  String valKey = valueMapIter.next();
					  
					  if( !(fieldsMap.containsKey(valKey)) )
					  {
						  valueMapIter.remove();
					  }
				  }
			  }
			  else
			  {
				  recordKeyIter.remove();
			  }
		  }
		} catch (JSONException e) {
			// ignore the record on JSON error
			LOGGER.log(Level.FINE, "{0} Problem getting guid from json: {1}",
						  new Object[]{app.getNodeID(), e.getMessage()});
			// there is some problem, we can't return this record in the result set.. 
			return null;
		}
	  return record;
  }
  
  /**
   * This function checks that the query issuer has read access to all query fields for 
   * {@code record}.
   * 
   * @param packet
   * @param record
   * @param app
   * @return
   * true if the acl check on query fields is satisfied. Otherwise, false.
   */
  private boolean aclCheckForQueryAttributes(SelectRequestPacket packet, JSONObject record,
		  	GNSApplicationInterface<String> app) 
  {
	  try 
	  {
		  String guid = record.getString(NameRecord.NAME.getName());
		  List<String> queryFields = getFieldsForQueryType(packet);
		  
		  NameRecord nr = new NameRecord(app.getDB(), record);
        
		  ResponseCode responseCode = NSAuthentication.signatureAndACLCheck(null, guid, null, 
				queryFields, packet.getReader(), null, null, MetaDataTypeName.READ_WHITELIST, app, true, nr);
		  
		  LOGGER.log(Level.FINE, "{0} ACL check for select: guid={0} queryFields={1} responsecode={3}",
                new Object[]{app.getNodeID(), guid, queryFields, responseCode});
		  if (responseCode.isOKResult()) {
			  return true;
		  }
      } catch (JSONException | InvalidKeyException | InvalidKeySpecException 
    		  	| SignatureException | NoSuchAlgorithmException | FailedDBOperationException 
    		  	| UnsupportedEncodingException e) 
	  {
        // ignore json errros
        LOGGER.log(Level.FINE, "{0} Problem getting guid from json: {1}",
                new Object[]{app.getNodeID(), e.getMessage()});
      }
	  return false;
  }
  
  /**
   * This filters individual fields if the cannot be accessed by the reader.
   *
   * @param packet
   * @param record
   * @param app
   * @return
   * the JSONObject after removing fields that don't satisfy ACL checks.
   */
  private JSONObject aclCheckForProjectionFields(SelectRequestPacket packet, JSONObject record,
		  GNSApplicationInterface<String> app) 
  {
	  try 
	  {
		  String guid = record.getString(NameRecord.NAME.getName());
		  // Look at the keys in the values map
		  JSONObject valuesMap = record.getJSONObject(NameRecord.VALUES_MAP.getName());
		  Iterator<?> keys = valuesMap.keys();
		  while (keys.hasNext()) 
		  {
			  String field = (String) keys.next();
			  if (!InternalField.isInternalField(field)) 
			  {
				  LOGGER.log(Level.FINE, "{0} Checking: {1}", new Object[]{app.getNodeID(), field});
				  ResponseCode responseCode = NSAuthentication.signatureAndACLCheck(null, guid, field, 
						  null, packet.getReader(), null, null, MetaDataTypeName.READ_WHITELIST, app, true, 
						  new NameRecord(app.getDB(), record));
            
				  if (!responseCode.isOKResult()) 
				  {
					  LOGGER.log(Level.FINE, "{0} Removing: {1}", new Object[]{app.getNodeID(), field});
					  // removing the offending field
					  keys.remove();
				  }
			  }
		  }
      } catch (JSONException | InvalidKeyException | InvalidKeySpecException 
    		  | SignatureException | NoSuchAlgorithmException | FailedDBOperationException 
    		  | UnsupportedEncodingException e) 
	  {
    	  LOGGER.log(Level.FINE, "{0} Problem getting guid from json: {1}",
                new Object[]{app.getNodeID(), e.getMessage()});
    	  // This record has problems, so we can't return this to a user. 
    	  return null;
      }
	  return record;
  }
  
  // Returns the fields that present in a query.
  private List<String> getFieldsForQueryType(SelectRequestPacket request) {
    switch (request.getSelectOperation()) {
      case EQUALS:
      case NEAR:
      case WITHIN:
        return new ArrayList<>(Arrays.asList(request.getKey()));
      case QUERY:
      case SELECT_NOTIFY:
        return getFieldsFromQuery(request.getQuery());
      default:
        return new ArrayList<>();
    }
  }

  // Uses a regular expression to extract the fields from a select query.
  private List<String> getFieldsFromQuery(String query) {
    List<String> result = new ArrayList<>();
    // Create a Pattern object
    Matcher m = Pattern.compile("~\\w+(\\.\\w+)*").matcher(query);
    // Now create matcher object.
    while (m.find()) {
      result.add(m.group().substring(1));
    }
    return result;
  }

  /**
   * Handles a select response.
   * This code runs in the collecting NS.
   *
   * @param packet
   * @param replica
   * @throws JSONException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws java.io.IOException
   * @throws InternalRequestException
   */
  @Override
  public void handleSelectResponse(SelectResponsePacket packet,
          GNSApplicationInterface<String> replica) throws JSONException, ClientException, 
  			IOException, InternalRequestException 
  {  
	  LOGGER.log(Level.FINE,
            "NS {0} recvd from NS {1}",
            new Object[]{replica.getNodeID(),
              packet.getNSAddress()});
	  NSSelectInfo info = QUERIES_IN_PROGRESS.get(packet.getNsQueryId());
	  if (info == null) 
	  {
		  LOGGER.log(Level.WARNING,
              "NS {0} unabled to located query info:{1}",
              new Object[]{replica.getNodeID(), packet.getNsQueryId()});
		  return;
	  }
	  // if there is no error update our results list
	  if (ResponseCode.NO_ERROR.equals(packet.getResponseCode())) 
	  {
		  switch(info.getSelectOperation())
		  {
		  	case EQUALS:
		  	case NEAR:
		  	case WITHIN:
		  	case QUERY:
		  	{
		  		processSelectResponseForReturningGUIDs();
		  		break;
		  	}
		  	case SELECT_NOTIFY:
		  	{
		  		processSelectResponseForSelectNotify();
		  		break;
		  	}
		  	case NOTIFICATION_STATUS:
		  	{
		  		processSelectResponseForNotificationStatus();
		  		break;
		  	}
		  	default:
		  	{
		  		break;
		  	}
		  }
		  storeReply(packet, info, replica);
	  } else 
	  {
		  // error response
		  LOGGER.log(Level.FINE,
              "NS {0} processing error response: {1}",
              new Object[]{replica.getNodeID(), packet.getErrorMessage()});
	  }
	  
	  // Remove the NS Address from the list to keep track of who has responded
	  boolean allServersResponded;
	  /* synchronization needed, otherwise assertion in app.sendToClient
	   * implying that an outstanding request is always found gets violated. 
	   */
	  synchronized (info) 
	  {
		  // Remove the NS Address from the list to keep track of who has responded
		  info.removeServerAddress(packet.getNSAddress());
		  allServersResponded = info.allServersResponded();
	  }
	  if (allServersResponded) 
	  {
		  handledAllServersResponded(PacketUtils.getInternalRequestHeader(packet), packet, info, replica);
	  } else 
	  {
		  LOGGER.log(Level.FINE,
              "NS{0} servers yet to respond:{1}",
              new Object[]{replica.getNodeID(), info.serversYetToRespond()});
	  }
  }
  
  // If all the servers have sent us a response we're done.
  private void handledAllServersResponded(InternalRequestHeader header,
          SelectResponsePacket packet, NSSelectInfo info,
          GNSApplicationInterface<String> replica) throws JSONException,
          ClientException, IOException, InternalRequestException 
  {
	  SelectResponsePacket response = null; 
	  switch(info.getSelectOperation())
	  {
	  	case EQUALS:
	  	case NEAR:
	  	case WITHIN:
	  	case QUERY:
	  	{
	  		Set<JSONObject> allRecords = info.getResponsesAsSet();
	  		Set<String> guids = extractGuidsFromRecords(allRecords);
	  		LOGGER.log(Level.FINE,
	              "NS{0} guids:{1}",
	              new Object[]{replica.getNodeID(), guids});
	  		
	  		// If projection is null we return guids (old-style).
	  		if (info.getProjection() == null) 
	  		{
	  			response = SelectResponsePacket.makeSuccessPacketForFullRecords(
	  				  packet.getId(), null, -1, null, new JSONArray(guids));
	  			// Otherwise we return a list of records.
	  		}
	  		else 
	  		{
	  			List<JSONObject> records = filterAndMassageRecords(allRecords);
	  			LOGGER.log(Level.FINE,
	                "NS{0} record:{1}",
	                new Object[]{replica.getNodeID(), records});
	  			response = SelectResponsePacket.makeSuccessPacketForFullRecords(packet.getId(),
	                null, -1, null, new JSONArray(records));
	  		}
	  		break;
	  	}
	  	case SELECT_NOTIFY:
	  	{
	  		List<NotificationStatsToIssuer> statsList = info.getAllNotificationStats();
	  		long totalNot = 0;
	  		long failedNot = 0;
	  		long pendingNot = 0;
	  		
	  		for(int i=0; i<statsList.size(); i++)
	  		{
	  			totalNot+=statsList.get(i).getTotalNotifications();
	  			failedNot+=statsList.get(i).getFailedNotifications();
	  			pendingNot+=statsList.get(i).getPendingNotifications();
	  		}
	  		
	  		// FIXME: Need to add the removal of state here, if all notifications
	  		// are sent, no pending notifications, then we can remove notifications. 
	  		// Entry-point name server sends out removal request to other name servers. 
	  		long localHandle 
	  					= entryPointState.addNotificationState(info.getAllServers());
	  		
	  		SelectHandleInfo selectHandle 
	  					= new SelectHandleInfo(localHandle, replica.getNodeAddress());
	  		
	  		
	  		NotificationStatsToIssuer mergedStats = new NotificationStatsToIssuer
	  							(selectHandle, totalNot, failedNot, pendingNot);
	  		
	  		
	  		response = SelectResponsePacket.makeSuccessPacketForNotificationStatsOnly
	  				(packet.getId(), null, -1, null, mergedStats);
	  		break;
	  	}
	  }
	  

	  // Put the result where the coordinator can see it.
	  QUERY_RESULT.put(packet.getNsQueryId(), response);
	  // and let the coordinator know the value is there
	  if (GNSApp.DELEGATE_CLIENT_MESSAGING) {
		  synchronized (QUERIES_IN_PROGRESS) {
    	  // Must be done after setting result in QUERY_RESULT,
    	  // Otherwise, the waiting thread will wake up and 
    	  // find the query is not in progress but will not find any result
    	  // so will return a wrong response. A wait()
    	  // can wake spuriously without the notify() as mentioned
    	  // in the documentation of these functions.
    	  QUERIES_IN_PROGRESS.remove(packet.getNsQueryId());
          QUERIES_IN_PROGRESS.notify();
		  }
	  }
  }

  // Converts a record from the database into something we can return to 
  // the user. Adds the "_GUID" and removes internal fields.
  protected List<JSONObject> filterAndMassageRecords(Set<JSONObject> records) {
    List<JSONObject> result = new ArrayList<>();
    for (JSONObject record : records) {
      try {
        JSONObject newRecord = new JSONObject();
        // add the _GUID record
        newRecord.put("_GUID", record.getString(NameRecord.NAME.getName()));
        //Now add all the non-internal records from the valuesMap
        JSONObject valuesMap = record.getJSONObject(NameRecord.VALUES_MAP.getName());
        Iterator<?> keyIter = valuesMap.keys();
        while (keyIter.hasNext()) {
          String key = (String) keyIter.next();
          if (!InternalField.isInternalField(key)) {
            newRecord.put(key, valuesMap.get(key));
          }
        }
        result.add(newRecord);
      } catch (JSONException e) {
      }
    }
    return result;
  }

  // Pulls the guids out of the record to return to the user for "old-style" 
  // select calls.
  protected  Set<String> extractGuidsFromRecords(Set<JSONObject> records) {
    Set<String> result = new HashSet<>();
    for (JSONObject json : records) {
      try {
        result.add(json.getString(NameRecord.NAME.getName()));
      } catch (JSONException e) {
      }
    }
    return result;
  }

  private int addQueryInfo(Set<InetSocketAddress> serverAddresses, SelectOperation selectOperation,
		  String query, List<String> projection) 
  {
	  int id;
	  do 
	  {
		  id = RANDOM_ID.nextInt();
	  } while (QUERIES_IN_PROGRESS.containsKey(id));
	  //Add query info
	  NSSelectInfo info = new NSSelectInfo(id, serverAddresses, selectOperation,
            query, projection);
	  QUERIES_IN_PROGRESS.put(id, info);
	  return id;
  }

  // Takes the JSON records that are returned from an NS and stuffs the into the NSSelectInfo record
  protected void storeReply(SelectResponsePacket packet, NSSelectInfo info, 
		  GNSApplicationInterface<String> ar) throws JSONException 
  {  
	  switch(info.getSelectOperation())
	  {
		  case EQUALS:
		  case NEAR:
		  case WITHIN:
		  case QUERY:
		  {
			  JSONArray jsonArray = packet.getRecords();
			  int length = jsonArray.length();
			  for (int i = 0; i < length; i++) 
			  {
				  JSONObject record = jsonArray.getJSONObject(i);
				  String name = record.getString(NameRecord.NAME.getName());
				  info.addRecordResponseIfNotSeenYet(name, record);
			  }
			  break;
		  }
		  case SELECT_NOTIFY:
		  {
			  info.addNotificationStat(packet.getNotificationStats());
			  break;
		  }
		  default:
			  LOGGER.log(Level.WARNING, "{0} No matching case in select response {1}"
					  	, new Object[]{ar.getNodeID(), packet});
			  break;
	  }
  }
  

  /**
 * @param args
 * @throws JSONException
 * @throws UnknownHostException
 */
public static void main(String[] args) throws JSONException, UnknownHostException {
    String testQuery = "$or: [{~geoLocationCurrent:{"
            + "$geoIntersects:{$geometry:{\"coordinates\":"
            + "[[[-98.08,33.635],[-96.01,33.635],[-96.01,31.854],[-98.08,31.854],[-98.08,33.635]]],"
            + "\"type\":\"Polygon\"}}}},"
            + "{~customLocations.location:{$geoIntersects:{$geometry:{\"coordinates\":"
            + "[[[-98.08,33.635],[-96.01,33.635],[-96.01,31.854],[-98.08,31.854],[-98.08,33.635]]],"
            + "\"type\":\"Polygon\"}}}},"
            + "{~customLocations.location:{$geoIntersects:{$geometry:{\"coordinates\":"
            + "[[[-98.08,33.635],[-96.01,33.635],[-96.01,31.854],[-98.08,31.854],[-98.08,33.635]]],"
            + "\"type\":$where}}}}"
            + "]";

    //System.out.println(getFieldsFromQuery(testQuery));

    System.out.println(queryContainsEvil(testQuery));

    String testQueryBad = "$or: [{~geoLocationCurrent:{"
            + "$geoIntersects:{$geometry:{\"coordinates\":"
            + "[[[-98.08,33.635],[-96.01,33.635],[-96.01,31.854],[-98.08,31.854],[-98.08,33.635]]],"
            + "\"type\":\"Polygon\"}}}},"
            + "{~customLocations.location:{$geoIntersects:{$geometry:{\"coordinates\":"
            + "[[[-98.08,33.635],[-96.01,33.635],[-96.01,31.854],[-98.08,31.854],[-98.08,33.635]]],"
            + "\"type\":\"Polygon\"}}}},"
            + "{~customLocations.location:{$geoIntersects:{$where:{\"coordinates\":"
            + "[[[-98.08,33.635],[-96.01,33.635],[-96.01,31.854],[-98.08,31.854],[-98.08,33.635]]],"
            + "\"type\":\"Polygon\"}}}}"
            + "]";

    //System.out.println(getFieldsFromQuery(testQueryBad));

    System.out.println(queryContainsEvil(testQueryBad));

    String testQuery3 = "nr_valuesMap.secret:{$regex : ^i_like_cookies}";
    System.out.println(queryContainsEvil(testQuery3));
    String testQuery4 = "$where : \"this.nr_valuesMap.secret == 'i_like_cookies'\"";
    System.out.println(queryContainsEvil(testQuery4));
  }
}
