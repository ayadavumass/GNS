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
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.json.JSONArray;
import java.util.ArrayList;
import org.json.JSONException;
import org.json.JSONObject;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsclient.client.tcp.AndroidNIOTask;
import edu.umass.cs.gnsclient.client.tcp.CommandResult;
import edu.umass.cs.gnsserver.gnsApp.packet.CommandPacket;
import edu.umass.cs.gnsserver.gnsApp.packet.CommandValueReturnPacket;
import edu.umass.cs.gnscommon.utils.ByteUtils;
import edu.umass.cs.gnscommon.utils.CanonicalJSON;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.client.util.Password;
import edu.umass.cs.gnsclient.client.util.Util;
import edu.umass.cs.gnsclient.exceptions.EncryptionException;
import edu.umass.cs.gnsclient.exceptions.GnsACLException;
import edu.umass.cs.gnsclient.exceptions.GnsDuplicateNameException;
import edu.umass.cs.gnsclient.exceptions.GnsException;
import edu.umass.cs.gnsclient.exceptions.GnsFieldNotFoundException;
import edu.umass.cs.gnsclient.exceptions.GnsInvalidFieldException;
import edu.umass.cs.gnsclient.exceptions.GnsInvalidGroupException;
import edu.umass.cs.gnsclient.exceptions.GnsInvalidGuidException;
import edu.umass.cs.gnsclient.exceptions.GnsInvalidUserException;
import edu.umass.cs.gnsclient.exceptions.GnsVerificationException;
import edu.umass.cs.gnsserver.gnsApp.packet.Packet;
import edu.umass.cs.nio.SSLDataProcessingWorker;
import static edu.umass.cs.nio.SSLDataProcessingWorker.SSL_MODES.CLEAR;
import static edu.umass.cs.nio.SSLDataProcessingWorker.SSL_MODES.SERVER_AUTH;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * This class defines a basic client to communicate with a GNS instance over TCP. This
 * class contains a concise set of read and write commands which read and write JSON Objects.
 * It also contains the basic field read and write commands as well a
 * set of commands to use context aware group guids.
 *
 * For a more complete set of commands see also {@link UniversalTcpClient} and {@link UniversalTcpClientExtended}.
 *
 * @author <a href="mailto:westy@cs.umass.edu">Westy</a>
 */
public class NewBasicUniversalTcpClient extends NewBaseClient implements GNSClientInterface {

  private InetSocketAddress remoteAddress;
  /**
   * Indicates whether we are on an Android platform or not
   */
  public static final boolean isAndroid = System.getProperty("java.vm.name")
          .equalsIgnoreCase("Dalvik");

  /**
   * The length of time we will wait for a command response from the server
   * before giving up.
   */
  // FIXME: We might need a separate timeout just for certain ops like 
  // gui creation that sometimes take a while
  // 10 seconds is too short on EC2 
  private int readTimeout = 15000; // 15 seconds... was 40 seconds

  /* Keeps track of requests that are sent out and the reponses to them */
  private final ConcurrentMap<Integer, CommandResult> resultMap = new ConcurrentHashMap<Integer, CommandResult>(
          10, 0.75f, 3);
  /* Instrumentation: Keeps track of transmission start times */
  private final ConcurrentMap<Integer, Long> queryTimeStamp = new ConcurrentHashMap<Integer, Long>(10,
          0.75f, 3);
  /* Used to generate unique ids */
  private final Random randomID = new Random();
  /* Used by the wait/notify calls */
  private final Object monitor = new Object();

  // Enables all the debug logging statements in the client.
  protected boolean debuggingEnabled = false;

  // When this is ture we don't use SSL.
  private final boolean disableSSL;

  // instrumentation
  private double movingAvgLatency;
  //private long lastLatency;
  private int totalAsynchErrors;

  /**
   * Creates a new <code>NewBasicUniversalTcpClient</code> object
   *
   * @param address
   * @throws java.io.IOException
   */
  public NewBasicUniversalTcpClient(InetSocketAddress address) throws IOException {
    this(new HashSet<>(Arrays.asList(address)), false);
  }

  /**
   * @throws IOException
   */
  public NewBasicUniversalTcpClient() throws IOException {
    this(ReconfigurationConfig.getReconfiguratorAddresses(), false);
  }

  /**
   * Creates a new <code>NewBasicUniversalTcpClient</code> object
   * Optionally disables SSL if disableSSL is true.
   *
   * @param addresses
   * @param disableSSL
   * @throws java.io.IOException
   */
  public NewBasicUniversalTcpClient(Set<InetSocketAddress> addresses, boolean disableSSL) throws IOException {
    super(new HashSet<>(Arrays.asList(Packet.PacketType.COMMAND, Packet.PacketType.COMMAND_RETURN_VALUE)),
            addresses);
    this.disableSSL = disableSSL;
    SSLDataProcessingWorker.SSL_MODES sslMode = disableSSL ? CLEAR : SERVER_AUTH;
    if (!disableSSL) {
      ReconfigurationConfig.setClientPortOffset(100);
    }
    if (debuggingEnabled) {
      System.out.println("SSL Mode is " + sslMode.name());
    }
    // CURRENTLY ONLY USED FOR INDEXING THE KEY PAIRS
    remoteAddress = addresses.iterator().next();
    resetInstrumentation();
  }

  public Long send(CommandPacket packet) throws IOException, JSONException {
    return sendRequest(packet, new ClientRequestCallback());
  }

  class ClientRequestCallback implements RequestCallback {

    @Override
    public void handleResponse(Request response) {
      long receivedTime = System.currentTimeMillis();
      try {
        JSONObject jsonObject = new JSONObject(response.toString());
        switch (Packet.getPacketType(jsonObject)) {
          case COMMAND:
            GNSClient.getLogger().severe("Should not have received a Command packet!!");
            break;
          case COMMAND_RETURN_VALUE:
            handleCommandValueReturnPacket(jsonObject, receivedTime);
            break;
          default:
            break;
        }
      } catch (JSONException e) {
        if (debuggingEnabled) {
          GNSClient.getLogger().severe("Unable to parse response: " + e);
        }
      }

    }

  }

  /**
   * Returns the remote host.
   *
   * @return
   */
  @Override
  public String getGnsRemoteHost() {
    return remoteAddress.getHostString();
  }

  /**
   * Returns the port.
   *
   * @return
   */
  @Override
  public int getGnsRemotePort() {
    return remoteAddress.getPort();
  }

  /**
   * Returns the timeout value (milliseconds) used when sending commands to the
   * server.
   *
   * @return value in milliseconds
   */
  public int getReadTimeout() {
    return readTimeout;
  }

  /**
   * Sets the timeout value (milliseconds) used when sending commands to the
   * server.
   *
   * @param readTimeout in milliseconds
   */
  public void setReadTimeout(int readTimeout) {
    this.readTimeout = readTimeout;
  }

  // READ AND WRITE COMMANDS
  /**
   * Updates the JSONObject associated with targetGuid using the given JSONObject.
   * Top-level fields not specified in the given JSONObject are not modified.
   * The writer is the guid of the user attempting access. Signs the query using
   * the private key of the user associated with the writer guid.
   *
   * @param targetGuid
   * @param json
   * @param writer
   * @throws IOException
   * @throws GnsException
   */
  public void update(String targetGuid, JSONObject json, GuidEntry writer) throws IOException, GnsException {
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), REPLACE_USER_JSON, GUID,
            targetGuid, USER_JSON, json.toString(), WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Replaces the JSON in guid with JSONObject. Signs the query using
   * the private key of the given guid.
   *
   * @param guid
   * @param json
   * @throws IOException
   * @throws GnsException
   */
  public void update(GuidEntry guid, JSONObject json) throws IOException, GnsException {
    NewBasicUniversalTcpClient.this.update(guid.getGuid(), json, guid);
  }

  /**
   * Updates the field in the targetGuid. The writer is the guid
   * of the user attempting access. Signs the query using
   * the private key of the writer guid.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws GnsException
   * @throws JSONException
   */
  public void fieldUpdate(String targetGuid, String field, Object value, GuidEntry writer) throws IOException, GnsException, JSONException {
    JSONObject json = new JSONObject();
    json.put(field, value);
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), REPLACE_USER_JSON, GUID,
            targetGuid, USER_JSON, json.toString(), WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Updates the field in the targetGuid.
   * Signs the query using the private key of the given guid.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsException
   * @throws JSONException
   */
  public void fieldUpdate(GuidEntry targetGuid, String field, Object value) throws IOException, GnsException, JSONException {
    fieldUpdate(targetGuid.getGuid(), field, value, targetGuid);
  }

  /**
   * Reads the JSONObject for the given targetGuid.
   * The reader is the guid of the user attempting access. Signs the query using
   * the private key of the user associated with the reader guid (unsigned if
   * reader is null).
   *
   * @param targetGuid
   * @param reader if null guid must be all fields readable for all users
   * @return a JSONObject
   * @throws Exception
   */
  public JSONObject read(String targetGuid, GuidEntry reader) throws Exception {
    JSONObject command;
    if (reader == null) {
      // this one actually uses the old style read... for now
      command = createCommand(READ_ARRAY, GUID, targetGuid, FIELD, ALL_FIELDS);
    } else {
      // this one actually uses the old style read... for now
      command = createAndSignCommand(reader.getPrivateKey(), READ_ARRAY, GUID, targetGuid, FIELD, ALL_FIELDS,
              READER, reader.getGuid());
    }

    String response = sendCommandAndWait(command);

    return new JSONObject(checkResponse(command, response));
  }

  /**
   * Reads the entire record from the GNS server for the given guid.
   * Signs the query using the private key of the guid.
   *
   * @param guid
   * @return a JSONArray containing the values in the field
   * @throws Exception
   */
  public JSONObject read(GuidEntry guid) throws Exception {
    return read(guid.getGuid(), guid);
  }

  /**
   * Returns true if the field exists in the given targetGuid.
   * Field is a string the naming the field. Field can use dot
   * notation to indicate subfields. The reader is the guid of the
   * user attempting access. This method signs the query using the
   * private key of the user associated with the reader guid (unsigned if
   * reader is null).
   *
   * @param targetGuid
   * @param field
   * @param reader if null the field must be readable for all
   * @return a boolean indicating if the field exists
   * @throws Exception
   */
  public boolean fieldExists(String targetGuid, String field, GuidEntry reader) throws Exception {
    JSONObject command;
    if (reader == null) {
      command = createCommand(READ, GUID, targetGuid, FIELD, field);
    } else {
      command = createAndSignCommand(reader.getPrivateKey(), READ, GUID, targetGuid, FIELD, field,
              READER, reader.getGuid());
    }
    String response = sendCommandAndWait(command);
    try {
      checkResponse(command, response);
      return true;
    } catch (GnsFieldNotFoundException e) {
      return false;
    }
  }

  /**
   * Returns true if the field exists in the given targetGuid.
   * Field is a string the naming the field. Field can use dot
   * notation to indicate subfields. This method signs the query using the
   * private key of the targetGuid.
   *
   * @param targetGuid
   * @param field
   * @return a boolean indicating if the field exists
   * @throws Exception
   */
  public boolean fieldExists(GuidEntry targetGuid, String field) throws Exception {
    return fieldExists(targetGuid.getGuid(), field, targetGuid);
  }

  /**
   * Reads the value of field for the given targetGuid.
   * Field is a string the naming the field. Field can use dot
   * notation to indicate subfields. The reader is the guid of the
   * user attempting access. This method signs the query using the
   * private key of the user associated with the reader guid (unsigned if
   * reader is null).
   *
   * @param targetGuid
   * @param field
   * @param reader if null the field must be readable for all
   * @return a string containing the values in the field
   * @throws Exception
   */
  public String fieldRead(String targetGuid, String field, GuidEntry reader) throws Exception {
    JSONObject command;
    if (reader == null) {
      command = createCommand(READ, GUID, targetGuid, FIELD, field);
    } else {
      command = createAndSignCommand(reader.getPrivateKey(), READ, GUID, targetGuid, FIELD, field,
              READER, reader.getGuid());
    }

    String response = sendCommandAndWait(command);

    return checkResponse(command, response);
  }

  /**
   * Reads the value of field from the targetGuid.
   * Field is a string the naming the field. Field can use dot
   * notation to indicate subfields. This method signs the query using the
   * private key of the targetGuid.
   *
   * @param targetGuid
   * @param field
   * @return
   * @throws Exception
   */
  public String fieldRead(GuidEntry targetGuid, String field) throws Exception {
    return fieldRead(targetGuid.getGuid(), field, targetGuid);
  }

  /**
   * Reads the value of fields for the given targetGuid.
   * Fields is a list of strings the naming the field. Fields can use dot
   * notation to indicate subfields. The reader is the guid of the
   * user attempting access. This method signs the query using the
   * private key of the user associated with the reader guid (unsigned if
   * reader is null).
   *
   * @param targetGuid
   * @param fields
   * @param reader if null the field must be readable for all
   * @return a JSONArray containing the values in the fields
   * @throws Exception
   */
  public String fieldRead(String targetGuid, ArrayList<String> fields, GuidEntry reader) throws Exception {
    JSONObject command;
    if (reader == null) {
      command = createCommand(READ, GUID, targetGuid, FIELDS, fields);
    } else {
      command = createAndSignCommand(reader.getPrivateKey(), READ, GUID, targetGuid,
              FIELDS, fields,
              READER, reader.getGuid());
    }

    String response = sendCommandAndWait(command);

    return checkResponse(command, response);
  }

  /**
   * Reads the value of fields for the given guid.
   * Fields is a list of strings the naming the field. Fields can use dot
   * notation to indicate subfields. This method signs the query using the
   * private key of the guid.
   *
   * @param targetGuid
   * @param fields
   * @return
   * @throws Exception
   */
  public String fieldRead(GuidEntry targetGuid, ArrayList<String> fields) throws Exception {
    return fieldRead(targetGuid.getGuid(), fields, targetGuid);
  }

  /**
   * Removes a field in the JSONObject record of the given targetGuid.
   * The writer is the guid of the user attempting access.
   * Signs the query using the private key of the user
   * associated with the writer guid.
   *
   * @param targetGuid
   * @param field
   * @param writer
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void fieldRemove(String targetGuid, String field, GuidEntry writer) throws IOException, InvalidKeyException,
          NoSuchAlgorithmException, SignatureException, GnsException {
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), REMOVE_FIELD, GUID, targetGuid,
            FIELD, field, WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);
    checkResponse(command, response);
  }

  // SELECT COMMANDS
  /**
   * Selects all records that match query.
   * Returns the result of the query as a JSONArray of guids.
   *
   * The query syntax is described here:
   * https://gns.name/wiki/index.php?title=Query_Syntax
   *
   * Currently there are two predefined field names in the GNS client (this is in edu.umass.cs.gnsclient.client.GnsProtocol):
   * LOCATION_FIELD_NAME = "geoLocation"; Defined as a "2d" index in the database.
   * IPADDRESS_FIELD_NAME = "netAddress";
   *
   * There are links in the wiki page abive to find the exact syntax for querying spacial coordinates.
   *
   * @param query - the query
   * @return - a JSONArray of guids
   * @throws Exception
   */
  public JSONArray selectQuery(String query) throws Exception {
    JSONObject command = createCommand(SELECT, QUERY, query);
    String response = sendCommandAndWait(command);

    return new JSONArray(checkResponse(command, response));
  }

  /**
   * Set up a context aware group guid using a query.
   * Requires a accountGuid and a publicKey which are used to set up the new guid
   * or look it up if it already exists.
   *
   * Also returns the result of the query as a JSONArray of guids.
   *
   * The query syntax is described here:
   * https://gns.name/wiki/index.php?title=Query_Syntax
   *
   * @param accountGuid
   * @param publicKey
   * @param query the query
   * @param interval - the refresh interval in seconds - default is 60 - (queries that happens quicker than
   * this will get stale results)
   * @return a JSONArray of guids
   * @throws Exception
   */
  public JSONArray selectSetupGroupQuery(GuidEntry accountGuid, String publicKey, String query, int interval) throws Exception {
    JSONObject command = createCommand(SELECT_GROUP, ACCOUNT_GUID, accountGuid.getGuid(),
            PUBLIC_KEY, publicKey, QUERY, query,
            INTERVAL, interval);
    String response = sendCommandAndWait(command);

    return new JSONArray(checkResponse(command, response));
  }

  /**
   * Look up the value of a context aware group guid using a query.
   * Returns the result of the query as a JSONArray of guids. The results will be
   * stale if the queries that happen more quickly than the refresh interval given during setup.
   *
   * @param guid
   * @return a JSONArray of guids
   * @throws Exception
   */
  public JSONArray selectLookupGroupQuery(String guid) throws Exception {
    JSONObject command = createCommand(SELECT_GROUP, GUID, guid);
    String response = sendCommandAndWait(command);

    return new JSONArray(checkResponse(command, response));
  }

  // ACCOUNT COMMANDS
  /**
   * Obtains the guid of the alias from the GNS server.
   *
   * @param alias
   * @return guid
   * @throws IOException
   * @throws UnsupportedEncodingException
   * @throws GnsException
   */
  public String lookupGuid(String alias) throws IOException, GnsException {
    JSONObject command = createCommand(LOOKUP_GUID, NAME, alias);
    String response = sendCommandAndWait(command);

    return checkResponse(command, response);
  }

  /**
   * If this is a sub guid returns the account guid it was created under.
   *
   * @param guid
   * @return
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws GnsException
   */
  public String lookupPrimaryGuid(String guid) throws UnsupportedEncodingException, IOException, GnsException {
    JSONObject command = createCommand(LOOKUP_PRIMARY_GUID, GUID, guid);
    String response = sendCommandAndWait(command);

    return checkResponse(command, response);
  }

  /**
   * Returns a JSON object containing all of the guid meta information.
   * This method returns meta data about the guid.
   * If you want any particular field or fields of the guid
   * you'll need to use one of the read methods.
   *
   * @param guid
   * @return
   * @throws IOException
   * @throws GnsException
   */
  @Override
  public JSONObject lookupGuidRecord(String guid) throws IOException, GnsException {
    JSONObject command = createCommand(LOOKUP_GUID_RECORD, GUID, guid);
    String response = sendCommandAndWait(command);
    checkResponse(command, response);
    try {
      return new JSONObject(response);
    } catch (JSONException e) {
      throw new GnsException("Failed to parse LOOKUP_GUID_RECORD response", e);
    }
  }

  /**
   * Returns a JSON object containing all of the account meta information for an
   * account guid.
   * This method returns meta data about the account associated with this guid
   * if and only if the guid is an account guid.
   * If you want any particular field or fields of the guid
   * you'll need to use one of the read methods.
   *
   * @param accountGuid
   * @return
   * @throws IOException
   * @throws GnsException
   */
  public JSONObject lookupAccountRecord(String accountGuid) throws IOException, GnsException {
    JSONObject command = createCommand(LOOKUP_ACCOUNT_RECORD, GUID, accountGuid);
    String response = sendCommandAndWait(command);
    checkResponse(command, response);
    try {
      return new JSONObject(response);
    } catch (JSONException e) {
      throw new GnsException("Failed to parse LOOKUP_ACCOUNT_RECORD response", e);
    }
  }

  /**
   * Get the public key for a given alias.
   *
   * @param alias
   * @return the public key registered for the alias
   * @throws GnsInvalidGuidException
   * @throws GnsException
   * @throws IOException
   */
  public PublicKey publicKeyLookupFromAlias(String alias) throws GnsInvalidGuidException, GnsException, IOException {

    String guid = lookupGuid(alias);
    return publicKeyLookupFromGuid(guid);
  }

  /**
   * Get the public key for a given GUID.
   *
   * @param guid
   * @return
   * @throws GnsInvalidGuidException
   * @throws GnsException
   * @throws IOException
   */
  public PublicKey publicKeyLookupFromGuid(String guid) throws GnsInvalidGuidException, GnsException, IOException {
    JSONObject guidInfo = lookupGuidRecord(guid);
    try {
      String key = guidInfo.getString(GUID_RECORD_PUBLICKEY);
      byte[] encodedPublicKey = Base64.decode(key);
      KeyFactory keyFactory = KeyFactory.getInstance(RSA_ALGORITHM);
      X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(encodedPublicKey);
      return keyFactory.generatePublic(publicKeySpec);
    } catch (JSONException e) {
      throw new GnsException("Failed to parse LOOKUP_USER response", e);
    } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
      throw new EncryptionException("Public key encryption failed", e);
    }

  }

  /**
   * Register a new account guid with the corresponding alias on the GNS server.
   * This generates a new guid and a public / private key pair. Returns a
   * GuidEntry for the new account which contains all of this information.
   *
   * @param alias - a human readable alias to the guid - usually an email
   * address
   * @return
   * @throws Exception
   */
  @Override
  public GuidEntry accountGuidCreate(String alias, String password) throws Exception {

    KeyPair keyPair = KeyPairGenerator.getInstance(RSA_ALGORITHM).generateKeyPair();
    String guid = GuidUtils.createGuidFromPublicKey(keyPair.getPublic().getEncoded());
    // Squirrel this away now just in case the call below times out.
    KeyPairUtils.saveKeyPair(remoteAddress.getHostString() + ":" + remoteAddress.getPort(), alias, guid, keyPair);
    String returnedGuid = accountGuidCreateHelper(alias, keyPair.getPublic(), keyPair.getPrivate(), password);
    assert returnedGuid.equals(guid);
    // Anything else we want to do here?
    if (!returnedGuid.equals(guid)) {
      GNSClient.getLogger().warning("Returned guid " + returnedGuid + "doesn't match locally created guid" + guid);
    }
    GuidEntry entry = new GuidEntry(alias, guid, keyPair.getPublic(), keyPair.getPrivate());

    return entry;
  }

  /**
   * Verify an account by sending the verification code back to the server.
   *
   * @param guid the account GUID to verify
   * @param code the verification code
   * @return ?
   * @throws Exception
   */
  @Override
  public String accountGuidVerify(GuidEntry guid, String code) throws Exception {
    JSONObject command = createAndSignCommand(guid.getPrivateKey(), VERIFY_ACCOUNT, GUID, guid.getGuid(),
            CODE, code);
    String response = sendCommandAndWait(command);
    return checkResponse(command, response);
  }

  /**
   * Deletes the account given by name
   *
   * @param guid GuidEntry
   * @throws Exception
   */
  public void accountGuidRemove(GuidEntry guid) throws Exception {
    JSONObject command = createAndSignCommand(guid.getPrivateKey(), REMOVE_ACCOUNT, GUID, guid.getGuid(),
            NAME, guid.getEntityName());
    String response = sendCommandAndWait(command);
    checkResponse(command, response);
  }

  /**
   * Creates an new GUID associated with an account on the GNS server.
   *
   * @param accountGuid
   * @param alias the alias
   * @return the newly created GUID entry
   * @throws Exception
   */
  @Override
  public GuidEntry guidCreate(GuidEntry accountGuid, String alias) throws Exception {

    long startTime = System.currentTimeMillis();
    GuidEntry entry = GuidUtils.createAndSaveGuidEntry(alias,
            remoteAddress.getHostString(), remoteAddress.getPort());
    DelayProfiler.updateDelay("updatePreferences", startTime);
    String returnedGuid = guidCreateHelper(accountGuid, alias, entry.getPublicKey());
    assert returnedGuid.equals(entry.getGuid());
    // Anything else we want to do here?
    if (!returnedGuid.equals(entry.getGuid())) {
      GNSClient.getLogger().warning("Returned guid " + returnedGuid + "doesn't match locally created guid" + entry.getGuid());
    }
    DelayProfiler.updateDelay("guidCreateFromAlias", startTime);
    return entry;
  }

  /**
   * Batch create guids with the given aliases. If
   * createPublicKeys is true, key pairs will be created and saved
   * by the client for the guids.
   *
   * @param accountGuid
   * @param aliases
   * @param createPublicKeys
   * @return
   * @throws Exception
   */
  public String guidBatchCreate(GuidEntry accountGuid, Set<String> aliases,
          boolean createPublicKeys) throws Exception {

    List<String> aliasList = new ArrayList<>(aliases);
    List<String> publicKeys = null;
    if (createPublicKeys) {
      long publicKeyStartTime = System.currentTimeMillis();
      publicKeys = new ArrayList<>();
      for (String alias : aliasList) {
        long singleEntrystartTime = System.currentTimeMillis();
        GuidEntry entry = GuidUtils.createAndSaveGuidEntry(alias,
                remoteAddress.getHostString(), remoteAddress.getPort());
        DelayProfiler.updateDelay("updateOnePreference", singleEntrystartTime);
        byte[] publicKeyBytes = entry.getPublicKey().getEncoded();
        String publicKeyString = Base64.encodeToString(publicKeyBytes, false);
        publicKeys.add(publicKeyString);
      }
      DelayProfiler.updateDelay("batchCreatePublicKeys", publicKeyStartTime);
    }

    System.out.println(DelayProfiler.getStats());
    JSONObject command;
    if (createPublicKeys) {
      command = createAndSignCommand(accountGuid.getPrivateKey(), ADD_MULTIPLE_GUIDS,
              ACCOUNT_GUID, accountGuid.getGuid(),
              NAMES, new JSONArray(aliasList),
              PUBLIC_KEYS, new JSONArray(publicKeys));
    } else {
      // This version creates guids that have bogus public keys
      command = createAndSignCommand(accountGuid.getPrivateKey(), ADD_MULTIPLE_GUIDS,
              ACCOUNT_GUID, accountGuid.getGuid(),
              NAMES, new JSONArray(aliasList));
    }
    String result = checkResponse(command, sendCommandAndWait(command));
    return result;
  }

  /**
   * Batch create a number guids. These guids will have
   * random aliases and can be accessed using the account guid.
   *
   * @param accountGuid
   * @param guidCnt
   * @return
   * @throws Exception
   */
  public String guidBatchCreateFast(GuidEntry accountGuid, int guidCnt) throws Exception {
    JSONObject command;
    // This version creates guids that have bogus public keys
    command = createAndSignCommand(accountGuid.getPrivateKey(), ADD_MULTIPLE_GUIDS,
            ACCOUNT_GUID, accountGuid.getGuid(),
            GUIDCNT, guidCnt);
    String result = checkResponse(command, sendCommandAndWait(command));
    return result;
  }

  /**
   * Removes a guid (not for account Guids - use removeAccountGuid for them).
   *
   * @param guid the guid to remove
   * @throws Exception
   */
  public void guidRemove(GuidEntry guid) throws Exception {
    JSONObject command = createAndSignCommand(guid.getPrivateKey(), REMOVE_GUID, GUID, guid.getGuid());
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Removes a guid given the guid and the associated account guid.
   *
   * @param accountGuid
   * @param guidToRemove
   * @throws Exception
   */
  public void guidRemove(GuidEntry accountGuid, String guidToRemove) throws Exception {
    JSONObject command = createAndSignCommand(accountGuid.getPrivateKey(), REMOVE_GUID, GUID, guidToRemove,
            ACCOUNT_GUID, accountGuid.getGuid());
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  // GROUP COMMANDS
  /**
   * Return the list of guids that are members of the group. Signs the query
   * using the private key of the user associated with the guid.
   *
   * @param groupGuid the guid of the group to lookup
   * @param reader the guid of the entity doing the lookup
   * @return the list of guids as a JSONArray
   * @throws IOException if a communication error occurs
   * @throws GnsException if a protocol error occurs or the list cannot be
   * parsed
   * @throws GnsInvalidGuidException if the group guid is invalid
   */
  public JSONArray groupGetMembers(String groupGuid, GuidEntry reader) throws IOException, GnsException,
          GnsInvalidGuidException {
    JSONObject command = createAndSignCommand(reader.getPrivateKey(), GET_GROUP_MEMBERS, GUID, groupGuid,
            READER, reader.getGuid());
    String response = sendCommandAndWait(command);

    try {
      return new JSONArray(checkResponse(command, response));
    } catch (JSONException e) {
      throw new GnsException("Invalid member list", e);
    }
  }

  /**
   * Return a list of the groups that the guid is a member of. Signs the query
   * using the private key of the user associated with the guid.
   *
   * @param guid the guid we are looking for
   * @param reader the guid of the entity doing the lookup
   * @return the list of groups as a JSONArray
   * @throws IOException if a communication error occurs
   * @throws GnsException if a protocol error occurs or the list cannot be
   * parsed
   * @throws GnsInvalidGuidException if the group guid is invalid
   */
  public JSONArray guidGetGroups(String guid, GuidEntry reader) throws IOException, GnsException,
          GnsInvalidGuidException {
    JSONObject command = createAndSignCommand(reader.getPrivateKey(), GET_GROUPS, GUID, guid,
            READER, reader.getGuid());
    String response = sendCommandAndWait(command);

    try {
      return new JSONArray(checkResponse(command, response));
    } catch (JSONException e) {
      throw new GnsException("Invalid member list", e);
    }
  }

  /**
   * Add a guid to a group guid. Any guid can be a group guid. Signs the query
   * using the private key of the user associated with the writer.
   *
   * @param groupGuid guid of the group
   * @param guidToAdd guid to add to the group
   * @param writer the guid doing the add
   * @throws IOException
   * @throws GnsInvalidGuidException if the group guid does not exist
   * @throws GnsException
   */
  public void groupAddGuid(String groupGuid, String guidToAdd, GuidEntry writer) throws IOException,
          GnsInvalidGuidException, GnsException {
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), ADD_TO_GROUP, GUID, groupGuid,
            MEMBER, guidToAdd, WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Add multiple members to a group
   *
   * @param groupGuid guid of the group
   * @param members guids of members to add to the group
   * @param writer the guid doing the add
   * @throws IOException
   * @throws GnsInvalidGuidException
   * @throws GnsException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public void groupAddGuids(String groupGuid, JSONArray members, GuidEntry writer) throws IOException,
          GnsInvalidGuidException, GnsException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), ADD_TO_GROUP, GUID, groupGuid,
            MEMBERS, members.toString(), WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Removes a guid from a group guid. Any guid can be a group guid. Signs the
   * query using the private key of the user associated with the writer.
   *
   * @param guid guid of the group
   * @param guidToRemove guid to remove from the group
   * @param writer the guid of the entity doing the remove
   * @throws IOException
   * @throws GnsInvalidGuidException if the group guid does not exist
   * @throws GnsException
   */
  public void groupRemoveGuid(String guid, String guidToRemove, GuidEntry writer) throws IOException,
          GnsInvalidGuidException, GnsException {
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), REMOVE_FROM_GROUP, GUID, guid,
            MEMBER, guidToRemove, WRITER, writer.getGuid());

    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Remove a list of members from a group
   *
   * @param guid guid of the group
   * @param members guids to remove from the group
   * @param writer the guid of the entity doing the remove
   * @throws IOException
   * @throws GnsInvalidGuidException
   * @throws GnsException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public void groupRemoveGuids(String guid, JSONArray members, GuidEntry writer) throws IOException,
          GnsInvalidGuidException, GnsException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), REMOVE_FROM_GROUP, GUID, guid,
            MEMBERS, members.toString(), WRITER, writer.getGuid());

    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Authorize guidToAuthorize to add/remove members from the group groupGuid.
   * If guidToAuthorize is null, everyone is authorized to add/remove members to
   * the group. Note that this method can only be called by the group owner
   * (private key required) Signs the query using the private key of the group
   * owner.
   *
   * @param groupGuid the group GUID entry
   * @param guidToAuthorize the guid to authorize to manipulate group membership
   * or null for anyone
   * @throws Exception
   */
  public void groupAddMembershipUpdatePermission(GuidEntry groupGuid, String guidToAuthorize) throws Exception {
    aclAdd(AccessType.WRITE_WHITELIST, groupGuid, GROUP_ACL, guidToAuthorize);
  }

  /**
   * Unauthorize guidToUnauthorize to add/remove members from the group
   * groupGuid. If guidToUnauthorize is null, everyone is forbidden to
   * add/remove members to the group. Note that this method can only be called
   * by the group owner (private key required). Signs the query using the
   * private key of the group owner.
   *
   * @param groupGuid the group GUID entry
   * @param guidToUnauthorize the guid to authorize to manipulate group
   * membership or null for anyone
   * @throws Exception
   */
  public void groupRemoveMembershipUpdatePermission(GuidEntry groupGuid, String guidToUnauthorize) throws Exception {
    aclRemove(AccessType.WRITE_WHITELIST, groupGuid, GROUP_ACL, guidToUnauthorize);
  }

  /**
   * Authorize guidToAuthorize to get the membership list from the group
   * groupGuid. If guidToAuthorize is null, everyone is authorized to list
   * members of the group. Note that this method can only be called by the group
   * owner (private key required). Signs the query using the private key of the
   * group owner.
   *
   * @param groupGuid the group GUID entry
   * @param guidToAuthorize the guid to authorize to manipulate group membership
   * or null for anyone
   * @throws Exception
   */
  public void groupAddMembershipReadPermission(GuidEntry groupGuid, String guidToAuthorize) throws Exception {
    aclAdd(AccessType.READ_WHITELIST, groupGuid, GROUP_ACL, guidToAuthorize);
  }

  /**
   * Unauthorize guidToUnauthorize to get the membership list from the group
   * groupGuid. If guidToUnauthorize is null, everyone is forbidden from
   * querying the group membership. Note that this method can only be called by
   * the group owner (private key required). Signs the query using the private
   * key of the group owner.
   *
   * @param groupGuid the group GUID entry
   * @param guidToUnauthorize the guid to authorize to manipulate group
   * membership or null for anyone
   * @throws Exception
   */
  public void groupRemoveMembershipReadPermission(GuidEntry groupGuid, String guidToUnauthorize) throws Exception {
    aclRemove(AccessType.READ_WHITELIST, groupGuid, GROUP_ACL, guidToUnauthorize);
  }

  // ACL COMMANDS
  /**
   * Adds to an access control list of the given field. The accesser can be a
   * guid of a user or a group guid or null which means anyone can access the
   * field. The field can be also be +ALL+ which means all fields can be read by
   * the reader. Signs the query using the private key of the user associated
   * with the guid.
   *
   * @param accessType a value from GnrsProtocol.AccessType
   * @param targetGuid guid of the field to be modified
   * @param field field name
   * @param accesserGuid guid to add to the ACL
   * @throws Exception
   * @throws GnsException if the query is not accepted by the server.
   */
  public void aclAdd(AccessType accessType, GuidEntry targetGuid, String field, String accesserGuid)
          throws Exception {
    aclAdd(accessType.name(), targetGuid, field, accesserGuid);
  }

  /**
   * Removes a GUID from an access control list of the given user's field on the
   * GNS server to include the guid specified in the accesser param. The
   * accesser can be a guid of a user or a group guid or null which means anyone
   * can access the field. The field can be also be +ALL+ which means all fields
   * can be read by the reader. Signs the query using the private key of the
   * user associated with the guid.
   *
   * @param accessType
   * @param guid
   * @param field
   * @param accesserGuid
   * @throws Exception
   * @throws GnsException if the query is not accepted by the server.
   */
  public void aclRemove(AccessType accessType, GuidEntry guid, String field, String accesserGuid)
          throws Exception {
    aclRemove(accessType.name(), guid, field, accesserGuid);
  }

  /**
   * Get an access control list of the given user's field on the GNS server to
   * include the guid specified in the accesser param. The accesser can be a
   * guid of a user or a group guid or null which means anyone can access the
   * field. The field can be also be +ALL+ which means all fields can be read by
   * the reader. Signs the query using the private key of the user associated
   * with the guid.
   *
   * @param accessType
   * @param guid
   * @param field
   * @param accesserGuid
   * @return list of GUIDs for that ACL
   * @throws Exception
   * @throws GnsException if the query is not accepted by the server.
   */
  public JSONArray aclGet(AccessType accessType, GuidEntry guid, String field, String accesserGuid)
          throws Exception {
    return aclGet(accessType.name(), guid, field, accesserGuid);
  }

  // ALIASES
  /**
   * Creates an alias entity name for the given guid. The alias can be used just
   * like the original entity name.
   *
   * @param guid
   * @param name - the alias
   * @throws Exception
   */
  public void addAlias(GuidEntry guid, String name) throws Exception {
    JSONObject command = createAndSignCommand(guid.getPrivateKey(), ADD_ALIAS, GUID, guid.getGuid(),
            NAME, name);
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Removes the alias for the given guid.
   *
   * @param guid
   * @param name - the alias
   * @throws Exception
   */
  public void removeAlias(GuidEntry guid, String name) throws Exception {
    JSONObject command = createAndSignCommand(guid.getPrivateKey(), REMOVE_ALIAS, GUID, guid.getGuid(),
            NAME, name);
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Retrieve the aliases associated with the given guid.
   *
   * @param guid
   * @return - a JSONArray containing the aliases
   * @throws Exception
   */
  public JSONArray getAliases(GuidEntry guid) throws Exception {
    JSONObject command = createAndSignCommand(guid.getPrivateKey(), RETRIEVE_ALIASES, GUID, guid.getGuid());

    String response = sendCommandAndWait(command);
    try {
      return new JSONArray(checkResponse(command, response));
    } catch (JSONException e) {
      throw new GnsException("Invalid alias list", e);
    }
  }

  // TAGS
  /**
   * Creates a tag to the tags of the guid.
   *
   * @param guid
   * @param tag
   * @throws Exception
   */
  @Override
  public void addTag(GuidEntry guid, String tag) throws Exception {
    JSONObject command = createAndSignCommand(guid.getPrivateKey(), ADD_TAG,
            GUID, guid.getGuid(), NAME, tag);
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Removes a tag from the tags of the guid.
   *
   * @param guid
   * @param tag
   * @throws Exception
   */
  public void removeTag(GuidEntry guid, String tag) throws Exception {
    JSONObject command = createAndSignCommand(guid.getPrivateKey(), REMOVE_TAG,
            GUID, guid.getGuid(), NAME, tag);
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Retrieves GUIDs that have been tagged with tag
   *
   * @param tag
   * @return
   * @throws Exception
   */
  public JSONArray retrieveTagged(String tag) throws Exception {
    JSONObject command = createCommand(DUMP, NAME, tag);
    String response = sendCommandAndWait(command);

    return new JSONArray(checkResponse(command, response));
  }

  /**
   * Removes all guids that have the corresponding tag. Removes the reverse
   * fields for the entity name and aliases. Note: doesn't remove all the
   * associated fields yet, though, so still a work in progress.
   *
   * @param tag
   * @throws Exception
   */
  public void clearTagged(String tag) throws Exception {
    JSONObject command = createCommand(CLEAR_TAGGED, NAME, tag);
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  // ///////////////////////////////
  // // PRIVATE METHODS BELOW /////
  // /////////////////////////////
  /**
   * Creates a new GUID associated with an account.
   *
   * @param accountGuid
   * @param name
   * @param publicKey
   * @return
   * @throws Exception
   */
  private String guidCreateHelper(GuidEntry accountGuid, String name, PublicKey publicKey) throws Exception {
    long startTime = System.currentTimeMillis();
    byte[] publicKeyBytes = publicKey.getEncoded();
    String publicKeyString = Base64.encodeToString(publicKeyBytes, false);
    JSONObject command = createAndSignCommand(accountGuid.getPrivateKey(), ADD_GUID,
            ACCOUNT_GUID, accountGuid.getGuid(),
            NAME, name,
            //GUID, guid,
            PUBLIC_KEY, publicKeyString);
    String result = checkResponse(command, sendCommandAndWait(command));
    DelayProfiler.updateDelay("guidCreate", startTime);
    return result;
  }

  /**
   * Register a new account guid with the corresponding alias and the given
   * public key on the GNS server. Returns a new guid.
   *
   * @param alias the alias to register (usually an email address)
   * @param publicKey the public key associate with the account
   * @return guid the GUID generated by the GNS
   * @throws IOException
   * @throws UnsupportedEncodingException
   * @throws GnsException
   * @throws GnsInvalidGuidException if the user already exists
   */
  private String accountGuidCreateHelper(String alias, PublicKey publicKey, PrivateKey privateKey, String password) throws UnsupportedEncodingException,
          IOException, GnsException, GnsInvalidGuidException, NoSuchAlgorithmException {
    JSONObject command;
    long startTime = System.currentTimeMillis();
    if (password != null) {
      command = createAndSignCommand(privateKey, REGISTER_ACCOUNT,
              NAME, alias,
              PUBLIC_KEY, Base64.encodeToString(publicKey.getEncoded(), false),
              PASSWORD, Base64.encodeToString(Password.encryptPassword(password, alias), false));
    } else {
      command = createAndSignCommand(privateKey, REGISTER_ACCOUNT,
              NAME, alias,
              PUBLIC_KEY, Base64.encodeToString(publicKey.getEncoded(), false));
    }
    String result = checkResponse(command, sendCommandAndWait(command));
    DelayProfiler.updateDelay("accountGuidCreate", startTime);
    return result;
  }

  private void aclAdd(String accessType, GuidEntry guid, String field, String accesserGuid) throws Exception {
    JSONObject command = createAndSignCommand(guid.getPrivateKey(), ACL_ADD, ACL_TYPE, accessType,
            GUID, guid.getGuid(), FIELD, field, ACCESSER, accesserGuid == null
                    ? ALL_USERS
                    : accesserGuid);
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  private void aclRemove(String accessType, GuidEntry guid, String field, String accesserGuid) throws Exception {
    JSONObject command = createAndSignCommand(guid.getPrivateKey(), ACL_REMOVE, ACL_TYPE, accessType,
            GUID, guid.getGuid(), FIELD, field, ACCESSER, accesserGuid == null
                    ? ALL_USERS
                    : accesserGuid);
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  private JSONArray aclGet(String accessType, GuidEntry guid, String field, String accesserGuid) throws Exception {
    JSONObject command = createAndSignCommand(guid.getPrivateKey(), ACL_RETRIEVE, ACL_TYPE, accessType,
            GUID, guid.getGuid(), FIELD, field, ACCESSER, accesserGuid == null
                    ? ALL_USERS
                    : accesserGuid);
    String response = sendCommandAndWait(command);
    try {
      return new JSONArray(checkResponse(command, response));
    } catch (JSONException e) {
      throw new GnsException("Invalid ACL list", e);
    }
  }

  //
  // Instrumentation
  //
  public int pingValue(int node1, int node2) throws Exception {
    JSONObject command = createCommand(PING_VALUE, N, node1, N2, node2);
    String response = sendCommandAndWait(command);
    return Integer.parseInt(response);
  }

  /**
   * Checks the response from a command request for proper syntax as well as
   * converting error responses into the appropriate thrown GNS exceptions.
   *
   * @param command
   * @param response
   * @return
   * @throws GnsException
   */
  public String checkResponse(JSONObject command, String response) throws GnsException {
    // System.out.println("response:" + response);
    if (response.startsWith(BAD_RESPONSE)) {
      String results[] = response.split(" ");
      // System.out.println("results length:" + results.length);
      if (results.length < 2) {
        throw new GnsException("Invalid bad response indicator: " + response + " Command: " + command.toString());
      } else if (results.length >= 2) {
        // System.out.println("results[0]:" + results[0]);
        // System.out.println("results[1]:" + results[1]);
        String error = results[1];
        // deal with the rest
        StringBuilder parts = new StringBuilder();
        for (int i = 2; i < results.length; i++) {
          parts.append(" ");
          parts.append(results[i]);
        }
        String rest = parts.toString();

        if (error.startsWith(BAD_SIGNATURE)) {
          throw new EncryptionException();
        }
        if (error.startsWith(BAD_GUID) || error.startsWith(BAD_ACCESSOR_GUID)
                || error.startsWith(DUPLICATE_GUID) || error.startsWith(BAD_ACCOUNT)) {
          throw new GnsInvalidGuidException(error + rest);
        }
        if (error.startsWith(DUPLICATE_FIELD)) {
          throw new GnsInvalidFieldException(error + rest);
        }
        if (error.startsWith(BAD_FIELD) || error.startsWith(FIELD_NOT_FOUND)) {
          throw new GnsFieldNotFoundException(error + rest);
        }
        if (error.startsWith(BAD_USER) || error.startsWith(DUPLICATE_USER)) {
          throw new GnsInvalidUserException(error + rest);
        }
        if (error.startsWith(BAD_GROUP) || error.startsWith(DUPLICATE_GROUP)) {
          throw new GnsInvalidGroupException(error + rest);
        }

        if (error.startsWith(ACCESS_DENIED)) {
          throw new GnsACLException(error + rest);
        }

        if (error.startsWith(DUPLICATE_NAME)) {
          throw new GnsDuplicateNameException(error + rest);
        }

        if (error.startsWith(VERIFICATION_ERROR)) {
          throw new GnsVerificationException(error + rest);
        }
        throw new GnsException("General command failure: " + error + rest);
      }
    }
    if (response.startsWith(NULL_RESPONSE)) {
      return null;
    } else {
      return response;
    }
  }

  /**
   * Creates a command object from the given action string and a variable
   * number of key and value pairs.
   *
   * @param action
   * @param keysAndValues
   * @return the query string
   * @throws edu.umass.cs.gnsclient.exceptions.GnsException
   */
  public JSONObject createCommand(String action, Object... keysAndValues) throws GnsException {
    long startTime = System.currentTimeMillis();
    try {
      JSONObject result = new JSONObject();
      String key;
      Object value;
      result.put(COMMANDNAME, action);
      for (int i = 0; i < keysAndValues.length; i = i + 2) {
        key = (String) keysAndValues[i];
        value = keysAndValues[i + 1];
        result.put(key, value);
      }
      DelayProfiler.updateDelay("createCommand", startTime);
      return result;
    } catch (JSONException e) {
      throw new GnsException("Error encoding message", e);
    }
  }

  /**
   * Creates a command object from the given action string and a variable
   * number of key and value pairs with a signature parameter. The signature is
   * generated from the query signed by the given guid.
   *
   * @param privateKey
   * @param action
   * @param keysAndValues
   * @return the query string
   * @throws GnsException
   */
  public JSONObject createAndSignCommand(PrivateKey privateKey, String action, Object... keysAndValues) throws GnsException {
    long startTime = System.currentTimeMillis();
    try {
      JSONObject result = createCommand(action, keysAndValues);
      String canonicalJSON = CanonicalJSON.getCanonicalForm(result);
      //String canonicalJSON = JSONUtils.getCanonicalJSONString(result);
      String signature = signDigestOfMessage(privateKey, canonicalJSON);
      //System.out.println("SIGNING THIS: " + canonicalJSON);
      result.put(SIGNATURE, signature);
      DelayProfiler.updateDelay("createAndSignCommand", startTime);
      return result;
    } catch (GnsException | NoSuchAlgorithmException | InvalidKeyException | SignatureException | JSONException e) {
      throw new GnsException("Error encoding message", e);
    }
  }

  /**
   * Signs a digest of a message using private key of the given guid.
   *
   * @param guid
   * @param message
   * @return a signed digest of the message string
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  private String signDigestOfMessage(PrivateKey privateKey, String message) throws NoSuchAlgorithmException,
          InvalidKeyException, SignatureException {
    long startTime = System.currentTimeMillis();
    Signature instance = Signature.getInstance(SIGNATURE_ALGORITHM);

    instance.initSign(privateKey);
    // instance.update(messageDigest);
    instance.update(message.getBytes());
    byte[] signature = instance.sign();
    String result = ByteUtils.toHex(signature);
    DelayProfiler.updateDelay("signDigestOfMessage", startTime);
    return result;
  }

  /**
   * Check that the connectivity with the host:port can be established
   *
   * @throws IOException throws exception if a communication error occurs
   */
  @Override
  public void checkConnectivity() throws IOException {
    int originalReadTimeout = getReadTimeout();
    setReadTimeout(7000);
    JSONObject command;
    try {
      command = createCommand(CONNECTION_CHECK);
      String commandResult = sendCommandAndWait(command);
      if (!commandResult.startsWith(OK_RESPONSE)) {
        String[] results = commandResult.split(" ");
        throw new IOException(results.length == 2 ? results[1] : commandResult);
      }
    } catch (GnsException e) {
      throw new IOException("Unable to create connectivity command.");
    } finally {
      setReadTimeout(originalReadTimeout);
    }
  }

  /**
   * Sends a TCP get with given queryString to the host specified by the
   * {@link host} field.
   *
   * @param command
   * @return result of get as a string
   * @throws IOException if an error occurs
   */
  public String sendCommandAndWait(JSONObject command) throws IOException {
    if (isAndroid) {
      return androidSendCommandAndWait(command);
    } else {
      return desktopSendCommmandAndWait(command);
    }
  }

  public int sendCommandNoWait(JSONObject command) throws IOException {
    if (isAndroid) {
      return androidSendCommandNoWait(command).getId();
    } else {
      return desktopSendCommmandNoWait(command);
    }
  }

  /**
   * Sends a TCP get with given queryString to the host specified by the
   * {@link host} field. Waits for the response packet to come back.
   *
   * @param command
   * @return result of get as a string
   * @throws IOException if an error occurs
   */
  private String desktopSendCommmandAndWait(JSONObject command) throws IOException {
    long startTime = System.currentTimeMillis();
    int id = desktopSendCommmandNoWait(command);
    // now we wait until the correct packet comes back
    try {
      if (debuggingEnabled) {
        GNSClient.getLogger().info("Waiting for query id: " + id);
      }
      synchronized (monitor) {
        long monitorStartTime = System.currentTimeMillis();
        while (!resultMap.containsKey(id) && (readTimeout == 0 || System.currentTimeMillis() - monitorStartTime < readTimeout)) {
          monitor.wait(readTimeout);
        }
        if (readTimeout != 0 && System.currentTimeMillis() - monitorStartTime >= readTimeout) {
          if (debuggingEnabled) {
            GNSClient.getLogger().info("Timeout after  (" + id + ") : " + command.toString());
          }
          return BAD_RESPONSE + " " + TIMEOUT;
        }
      }
      if (debuggingEnabled) {
        GNSClient.getLogger().info("Query id response received: " + id);
      }
    } catch (InterruptedException x) {
      GNSClient.getLogger().severe("Wait for return packet was interrupted " + x);
    }
    CommandResult result = resultMap.remove(id);
    if (debuggingEnabled) {
      GNSClient.getLogger().info(
              String.format(
                      "Command name: %19s %40s %16s id: %12s "
                      + "NS: %16s ",
                      command.optString(COMMANDNAME, "Unknown"),
                      command.optString(GUID, ""),
                      command.optString(NAME, ""),
                      id,
                      result.getResponder()
              ));
    }
    DelayProfiler.updateDelay("desktopSendCommmand", startTime);
    if (debuggingEnabled) {
      GNSClient.getLogger().info(DelayProfiler.getStats());
    }
    return result.getResult();
  }

  private int desktopSendCommmandNoWait(JSONObject command) throws IOException {
    long startTime = System.currentTimeMillis();
    int id = generateNextRequestID();
    CommandPacket packet = new CommandPacket(id, null, -1, command);
    queryTimeStamp.put(id, startTime);
    sendCommandPacket(packet);
    DelayProfiler.updateDelay("desktopSendCommmandNoWait", startTime);
    return id;
  }

  private String androidSendCommandAndWait(JSONObject command) throws IOException {
    final AndroidNIOTask sendTask = androidSendCommandNoWait(command);
    try {
      return sendTask.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
  }

  private AndroidNIOTask androidSendCommandNoWait(JSONObject command) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.

//    final AndroidNIOTask sendTask = new AndroidNIOTask();
//    sendTask.setId(generateNextRequestID()); // so we can get it back from the task later
//    sendTask.execute(messenger, command, sendTask.getId(), remoteAddress, monitor,
//            queryTimeStamp, resultMap, readTimeout);
//    return sendTask;
  }

  /**
   * Called when a command value return packet is received.
   *
   * @param json
   * @param receivedTime
   * @throws JSONException
   */
  public void handleCommandValueReturnPacket(JSONObject json, long receivedTime) throws JSONException {
    long methodStartTime = System.currentTimeMillis();
    CommandValueReturnPacket packet = new CommandValueReturnPacket(json);
    int id = packet.getClientRequestId();
    // *INSTRUMENTATION*
    long queryStartTime = queryTimeStamp.remove(id);
    long latency = receivedTime - queryStartTime;
    movingAvgLatency = Util.movingAverage(latency, movingAvgLatency);
    // *END OF INSTRUMENTATION*
    if (debuggingEnabled) {
      GNSClient.getLogger().info("Handling return packet: " + json.toString());
    }
    // store the response away
    resultMap.put(id, new CommandResult(packet, receivedTime, latency));
    // This differentiates between packets sent synchronusly and asynchronusly
    if (!pendingAsynchPackets.containsKey(id)) {
      // for synchronus sends we notify waiting threads
      synchronized (monitor) {
        monitor.notifyAll();
      }
    } else {
      // Handle the asynchronus packets
      // note that we have recieved the reponse
      pendingAsynchPackets.remove(id);
      // * INSTRUMENTATION *
      // Record errors 
      if (packet.getErrorCode().isAnError()) {
        totalAsynchErrors++;
      }
    }
    DelayProfiler.updateCount("handleCommandValueReturnPacket", 1);
    DelayProfiler.updateDelay("handleCommandValueReturnPacket", methodStartTime);
  }

  public synchronized int generateNextRequestID() {
    int id;
    do {
      id = randomID.nextInt();
      // this is actually wrong because we can still generate duplicate keys
      // because the resultMap doesn't contain pending requests until they come back
    } while (resultMap.containsKey(id));
    return id;
  }

  /**
   * Returns true if a response has been received.
   *
   * @param id
   * @return
   */
  public boolean isAsynchResponseReceived(int id) {
    return resultMap.containsKey(id);
  }

  /**
   * Removes and returns the command result.
   *
   * @param id
   * @return
   */
  public CommandResult removeAsynchResponse(int id) {
    return resultMap.remove(id);
  }

// ASYNCHRONUS OPERATIONS
  /**
   * This contains all the command packets sent out asynchronously that have
   * not been acknowledged yet.
   */
  private final ConcurrentHashMap<Integer, CommandPacket> pendingAsynchPackets
          = new ConcurrentHashMap<>();

  public int outstandingAsynchPacketCount() {
    return pendingAsynchPackets.size();
  }

  /**
   * Sends a command packet without waiting for a response.
   * Performs bookkeeping so we can retrieve the response.
   *
   * @param packet
   * @throws IOException
   */
  public void sendCommandPacketAsynch(CommandPacket packet) throws IOException {
    long startTime = System.currentTimeMillis();
    int id = packet.getClientRequestId();
    pendingAsynchPackets.put(id, packet);
    queryTimeStamp.put(id, System.currentTimeMillis());
    sendCommandPacket(packet);
    DelayProfiler.updateDelay("sendAsynchTestCommand", startTime);
  }

  /**
   * Updates the field in the targetGuid without waiting for a response.
   * The writer is the guid
   * of the user attempting access. Signs the query using
   * the private key of the writer guid.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws GnsException
   * @throws JSONException
   */
  public void fieldUpdateAsynch(String targetGuid, String field, Object value, GuidEntry writer) throws GnsException, IOException, JSONException {
    JSONObject json = new JSONObject();
    json.put(field, value);
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), REPLACE_USER_JSON, GUID,
            targetGuid, USER_JSON, json.toString(), WRITER, writer.getGuid());
    sendCommandNoWait(command);
  }

  /**
   * Updates the field in the targetGuid without waiting for a response.
   * Signs the query using the private key of the given guid.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsException
   * @throws JSONException
   */
  public void fieldUpdateAsynch(GuidEntry targetGuid, String field, Object value) throws GnsException, IOException, JSONException {
    fieldUpdateAsynch(targetGuid.getGuid(), field, value, targetGuid);
  }

  private void sendCommandPacket(CommandPacket packet) throws IOException {
    long startTime = System.currentTimeMillis();
    try {
      if (disableSSL) {
        send(packet);
        //this.messenger.send(new GenericMessagingTask<>(remoteAddress, packet.toJSONObject()));
      } else {
        send(packet);
//        InetSocketAddress clientFacingAddress = new InetSocketAddress(remoteAddress.getAddress(),
//                ActiveReplica.getClientFacingPort(remoteAddress.getPort()));
//        this.messenger.sendToAddress(clientFacingAddress, packet.toJSONObject());
//        if (debuggingEnabled) {
//          GNSClient.getLogger().info("Sent packet to " + clientFacingAddress.toString()
//                  + " :" + packet.toString());
//        }
      }
      DelayProfiler.updateDelay("sendCommandPacket", startTime);
    } catch (JSONException e) {
      GNSClient.getLogger().severe("Problem sending JSON packet: " + e);
    }
  }

  public void resetInstrumentation() {
    movingAvgLatency = 0;
  }

  public boolean isDebuggingEnabled() {
    return this.debuggingEnabled;
  }

  public void setDebuggingEnabled(boolean debuggingEnabled) {
    this.debuggingEnabled = debuggingEnabled;
  }

  /**
   * Instrumentation. Returns the moving average of request latency
   * as seen by the client.
   *
   * @return
   */
  public double getMovingAvgLatency() {
    return movingAvgLatency;
  }

  /**
   * Instrumentation. Currently only valid when asynch testing.
   *
   * @return
   */
  public int getTotalAsynchErrors() {
    return totalAsynchErrors;
  }

}