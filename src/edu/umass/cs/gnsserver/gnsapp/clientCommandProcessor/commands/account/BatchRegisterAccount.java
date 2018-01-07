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
 *  Initial developer(s): ayadav
 *
 */
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSAccessSupport;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnsserver.utils.JSONUtils;
import edu.umass.cs.utils.Util;

/**
 *
 * @author ayadav
 */
public class BatchRegisterAccount extends AbstractCommand {

  /**
   * Creates a RegisterAccount instance.
   *
   * @param module
   */
  public BatchRegisterAccount(CommandModule module) {
    super(module);
  }

  /**
   *
   * @return the command type
   */
  @Override
  public CommandType getCommandType() {
    return CommandType.BatchRegisterAccount;
  }

  @Override
  public CommandResponse execute(InternalRequestHeader header, CommandPacket commandPacket, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, UnsupportedEncodingException,
          InternalRequestException {
	  
    JSONObject json = commandPacket.getCommand();
    
    JSONArray names = json.optJSONArray(GNSProtocol.NAMES.toString());
    JSONArray guids = json.optJSONArray(GNSProtocol.GUIDS.toString());
    String publicKey = json.getString(GNSProtocol.PUBLIC_KEY.toString());
    String password = json.getString(GNSProtocol.PASSWORD.toString());
    String signature = json.getString(GNSProtocol.SIGNATURE.toString());
    String message = json.getString(GNSProtocol.SIGNATUREFULLMESSAGE.toString());
	
	
    Set<InetSocketAddress> activesSet = json.has(GNSProtocol.ACTIVES_SET.toString())
    		? Util.getSocketAddresses(json.getJSONArray(GNSProtocol.ACTIVES_SET.toString()))
    		: null;
    
    if (!NSAccessSupport.verifySignature(publicKey, signature, message)) {
    	return new CommandResponse(ResponseCode.SIGNATURE_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_SIGNATURE.toString());
    }
    
    try {
    	CommandResponse result = AccountAccess.addAccountsInBatch(header, commandPacket,
    			handler.getHttpServerHostPortString(), JSONUtils.JSONArrayToArrayListString(names), 
    			JSONUtils.JSONArrayToArrayListString(guids),
    			publicKey, password, false,
    	        handler, activesSet);
    	
      if (result.getExceptionOrErrorCode().isOKResult()) {
        return new CommandResponse(ResponseCode.NO_ERROR, "BatchAccountCreation");
      } else {
        assert (result.getExceptionOrErrorCode() != null);
        // Otherwise return the error response.
        return result;
      }
    } catch (ClientException | IOException e) {
      return new CommandResponse(ResponseCode.UNSPECIFIED_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.UNSPECIFIED_ERROR.toString() + " " + e.getMessage());
    }
  }

}