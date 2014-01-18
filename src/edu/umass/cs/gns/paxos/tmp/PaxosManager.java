package edu.umass.cs.gns.paxos.tmp;

import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaController;
import edu.umass.cs.gns.nio.ByteStreamToJSONObjects;
import edu.umass.cs.gns.nio.NioServer;
import edu.umass.cs.gns.nio.PacketDemultiplexer;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.paxospacket.FailureDetectionPacket;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import edu.umass.cs.gns.packet.paxospacket.StatePacket;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages a set of Paxos instances, each instance could be running 
 * on a different subset of nodes but including this node.
 * @author abhigyan
 *
 */
public class PaxosManager extends Thread{

  static final String PAXOS_ID = "PXS";

  private final static String REQUEST_ID = "RQID";

//    public static final int timeoutForRequestState = 1000;

  /**
   *  total number of nodes. node IDs = 0, 1, ..., N -1
   */
  static int N;

  /**
   * nodeID of this node. among node IDs = 0, 1, ..., (N - 1)
   */
  static int nodeID;

  /**
   * When paxos is run independently {@code tcpTransport} is used to send messages between paxos replicas and client.
   */
  static NioServer tcpTransport;

  /**
   * Object stores all paxos instances.
   *
   * V. IMP.: The paxosID of a  {@code PaxosReplica} is stored in its field 'paxosID'. The key that this map uses for a
   * {@code PaxosReplica} is given by the method {@code getPaxosKeyFromPaxosID}.
   *
   * The paxosID for the paxos between primaries is defined as 'name-P', its key in this map is also 'name-P'.
   * The paxosID for the paxos between actives is defined as 'name-X', where X is an int. Its key in this map is just
   * 'name'. X changes every time the set of active replicas change. Defining the keys in this manner allows us to find
   * the paxos instance among actives for a name, without knowing what the current 'X' is. Otherwise proposing a
   * request would have needed an additional database lookup to find what the current X is for this name.
   *
   */
  static ConcurrentHashMap<String, PaxosReplicaInterface> paxosInstances = new ConcurrentHashMap<String, PaxosReplicaInterface>();


  static PaxosInterface clientRequestHandler;

  static ScheduledThreadPoolExecutor executorService;

  static int maxThreads = 5;

  /**
   * debug = true is used to debug the paxos module,  debug = false when complete GNRS system is running.
   */
  static boolean debug = false;
  /**
   * Paxos ID of the paxos instance created for testing/debugging.
   */
  static String defaultPaxosID  = "0-P";

  /**
   * Minimum interval (in milliseconds) between two garbage state collections of replicas.
   */
  static int MEMORY_GARBAGE_COLLECTION_INTERVAL = 100;

  /**
   * Paxos coordinator checks whether all replicas have received the latest messages decided.
   */
  static long RESEND_PENDING_MSG_INTERVAL_MILLIS = 2000;

  static long INIT_SCOUT_DELAY_MILLIS = 1000;

  static final TreeSet<ProposalStateAtCoordinator> proposalStates = new TreeSet<ProposalStateAtCoordinator>();
//  static ConcurrentSkipListMap<ProposalStateAtCoordinator,ProposalStateAtCoordinator> proposalStates =
//          new ConcurrentSkipListMap<ProposalStateAtCoordinator, ProposalStateAtCoordinator>();

  /**
   * Paxos logs are garbage collected at this interval
   */
  private static long PAXOS_LOG_STATE_INTERVAL_SEC = 100000;

  /**
   * Redundant paxos logs are checked and deleted at this interval.
   */
  private static long PAXOS_LOG_DELETE_INTERVAL_SEC = 100000;

  /**
   * variable is true if paxos recovery is complete
   */
  private static boolean recoveryComplete = false;

  /**
   * object used to synchronize access to {@code recoveryComplete}
   */
  private static Object recoveryCompleteLock = new ReentrantLock();

  /**
   * @return true if paxos has completed recovery, false otherwise
   */
  public static boolean isRecoveryComplete() {
    synchronized (recoveryCompleteLock) {
      return recoveryComplete;
    }
  }

  /**
   * method used to change the status of {@code recoveryComplete} object
   * @param status
   */
  private static void setRecoveryComplete(boolean status) {
    synchronized (recoveryCompleteLock) {
      recoveryComplete = status;
    }
  }

  public static void initializePaxosManager(int numberOfNodes, int nodeID, NioServer tcpTransport, PaxosInterface outputHandler, ScheduledThreadPoolExecutor executorService) {

    PaxosManager.N = numberOfNodes;
    PaxosManager.nodeID = nodeID;
    PaxosManager.tcpTransport = tcpTransport;

    PaxosManager.clientRequestHandler = outputHandler;

    PaxosManager.executorService = executorService;

    FailureDetection.initializeFailureDetection(N, nodeID);

    // recover previous state if exists using logger
//        ConcurrentHashMap<String, PaxosReplica> paxosInstances = PaxosLogger.initializePaxosLogger();
    // step 1: do local log recovery

    ConcurrentHashMap<String, PaxosReplicaInterface> myPaxosInstances = PaxosLogger.initLogger(nodeID);
    // TODO Fix below
//     paxosInstances = myPaxosInstances;
    // TODO Fix up

//    paxosInstances = new ConcurrentHashMap<PaxosName, PaxosReplica>();
//    if (recoveredPaxosInstances != null) {
//      for (String x: recoveredPaxosInstances.keySet()) {
//        GNS.getLogger().severe("putting " + PaxosManager.getPaxosKeyFromPaxosID(x));
//        paxosInstances.put(PaxosManager.getPaxosKeyFromPaxosID(x),recoveredPaxosInstances.get(x));
//      }
//    }
    if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos instances: " + paxosInstances.size());

//      if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos instances slot : " + paxosInstances.get(defaultPaxosID).getSlotNumber());
//      if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos instances ballot: " + paxosInstances.get(defaultPaxosID).getAcceptorBallot());

    if (debug && paxosInstances.size() == 0) createDefaultPaxosInstance();
    else startAllPaxosReplicas();

    // step 2: start global log synchronization: what do we
    //
//      startLogSynchronization();

    // step 3: start check whether global log sync is over
    // TODO finish step 2 and step 3

    startPaxosMaintenanceActions();

    if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos manager initialization complete");

  }

  public static void initializePaxosManagerDebugMode(String nodeConfig, String testConfig, int nodeID, PaxosInterface outputHandler) {
    // set debug mode to true
    debug = true;
    // read node configs

    readTestConfigFile(testConfig);

    PaxosManager.nodeID = nodeID;

    // init paxos manager
    initializePaxosManager(N, nodeID, initTransport(nodeConfig), outputHandler, new ScheduledThreadPoolExecutor(maxThreads));
//
//
////        ConcurrentHashMap<String, PaxosReplica> paxosInstances = PaxosLogger.initializePaxosLogger();
////        PaxosLogger.initLogger();
//
//    // initialize executor service
//
//
//    if (paxosInstances!=null) {
//      PaxosManager.paxosInstances = paxosInstances;
//      for (String x: PaxosManager.paxosInstances.keySet()) {
//        if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos Recovery: starting paxos replica .. " + x);
//        PaxosManager.paxosInstances.get(x).startReplica();
//      }
//    }
//
//    FailureDetection.initializeFailureDetection(N, nodeID);
//    // create a paxos instance for debugging
//    createDefaultPaxosInstance();


  }


  /**
   * Set the failure detection ping interval to <code>intervalMillis</code>
   * @param intervalMillis
   */
  public static void setFailureDetectionPingInterval(int intervalMillis) {
    FailureDetection.pingIntervalMillis = intervalMillis;
  }

  /**
   * Set the failure detection timeout to <code>intervalMillis</code>
   * @param intervalMillis
   */
  public static void setFailureDetectionTimeoutInterval(int intervalMillis) {
    FailureDetection.timeoutIntervalMillis = intervalMillis;

  }

  /**
   * set the paxos log folder to given value
   * @param paxosLogFolder
   */
  public static void setPaxosLogFolder(String paxosLogFolder) {
    PaxosLogger.setLoggerParameters(paxosLogFolder);

  }



  /**
   *
   */
  private static ConcurrentHashMap<Integer, Integer> globalSyncNodesResponded;

  private static ConcurrentHashMap<String, String> newPaxosInstancesUninitialized;


  /**
   * On startup, this method start to synchronize logs of this paxos instance with other paxos instances
   */
  private static void startLogSynchronization() {
    globalSyncNodesResponded = new ConcurrentHashMap<Integer, Integer>();
    newPaxosInstancesUninitialized = new ConcurrentHashMap<String, String>();
    // TODO implement this method
//    sendPaxosInstanceListToOtherNodes();
  }

  /**
   * set recovery complete to true if we have received synchronized list of paxos instances
   * with all nodes that have not failed, and all nodes have responded.
   */
  static void checkLogSynchronizationOver() {
    // TODO write this method
    startPaxosMaintenanceActions();
  }


  /**
   * Once recovery is complete (@code recoveryComplete) = true, this method starts the following actions:
   * 1. garbage collection of paxos logs
   * 2. logging state for all paxos instances to disk periodically
   * 3. resend any lost messages for paxos replicas
   */
  private static void startPaxosMaintenanceActions() {
    startPaxosLogDeletionTask();
    startPaxosStateLogging();

    startResendPendingMessages();


  }

  /**
   * delete paxos logs that are useless
   */
  private static void startPaxosLogDeletionTask() {
    LogDeletionTask delTask = new LogDeletionTask();
    executorService.scheduleAtFixedRate(delTask,(long)(0.5 + new Random().nextDouble())* PAXOS_LOG_DELETE_INTERVAL_SEC,
            PAXOS_LOG_DELETE_INTERVAL_SEC, TimeUnit.SECONDS);
  }


  /**
   *
   */
  private static void startPaxosStateLogging() {
    LogPaxosStateTask logTask = new LogPaxosStateTask();
    executorService.scheduleAtFixedRate(logTask,(long)(0.5 + new Random().nextDouble())*PAXOS_LOG_STATE_INTERVAL_SEC,
            PAXOS_LOG_STATE_INTERVAL_SEC, TimeUnit.SECONDS);
  }

  /**
   *
   */
  static void startResendPendingMessages() {
    ResendPendingMessagesTask task = new ResendPendingMessagesTask();
    // single time execution
    executorService.schedule(task, RESEND_PENDING_MSG_INTERVAL_MILLIS,TimeUnit.MILLISECONDS);
  }

  /**
   * set recovery complete to true if we have received synchronized list of Paxos instances
   * with all nodes that have not failed, and all nodes have responded.
   */
  private static void startAllPaxosReplicas() {
    if (paxosInstances!=null) {
      for (String x: PaxosManager.paxosInstances.keySet()) {
        if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos Recovery: starting paxos replica .. " + x);
        PaxosManager.paxosInstances.get(x).checkCoordinatorFailure();
      }
    }

  }

  /**
   *
   */
  private static void sendPaxosInstanceListToOtherNodes(ConcurrentHashMap<String, PaxosReplica> paxosInstances) {
    for (int i = 0; i < N; i++) {
      Set<String> paxosIDsActive = new HashSet<String>();
      Set<String> paxosIDsDeleted = new HashSet<String>();

      for (String x: paxosInstances.keySet()) {
        if (!paxosInstances.get(x).getNodeIDs().contains(i)) continue;
        if (paxosInstances.get(x).isStopped()) paxosIDsDeleted.add(x);
        else paxosIDsActive.add(x);
      }
      // send paxos IDs to node i
    }

  }

  private static void prepareListOfPaxosInstances(int senderNodeID, Set<String> paxosIDsActiveAtSendingNode,
                                                  Set<String> paxosIDsStoppedAtSendingNode) {
    throw  new UnsupportedOperationException();
////    synchronized (paxosInstances) {
////      HashMap<String,Set<Integer>> paxosInstancesAdded = new HashMap<String, Set<Integer>>();
////      HashMap<String,Set<Integer>> paxosInstancesStopped = new HashMap<String,Set<Integer>>();
////      HashMap<String,RequestPacket> stopRequests = new HashMap<String,RequestPacket>();
////
////      for (PaxosName paxosID: paxosInstances.keySet()) {
////        //
////        if (!paxosInstances.get(paxosID).isNodeInPaxosInstance(senderNodeID)) continue;
////
////        PaxosReplica paxosReplica = paxosInstances.get(paxosID);
////        if (paxosReplica.isStopped()) {
////          if (!paxosIDsStoppedAtSendingNode.contains(paxosID)) {
////            paxosInstancesStopped.put(paxosID, paxosReplica.getNodeIDs());
////            stopRequests.put(paxosID,paxosReplica.getLastRequest());
////          }
////        }
////        else {
////          if (!paxosIDsActiveAtSendingNode.contains(paxosID))
////            paxosInstancesAdded.put(paxosID,paxosReplica.getNodeIDs());
////        }
////
////      }
//    }
  }

  /**
   * Handle newPaxosInstancesAdded(ConcurrentHashMap<String, P)
   * @param paxosInstancesAdded
   */
  private static void handlePaxosInstanceSetAdded(ConcurrentHashMap<String, Set<Integer>> paxosInstancesAdded) {
    throw  new UnsupportedOperationException();
//    synchronized (paxosInstances) {
//      for (String paxosID: paxosInstancesAdded.keySet()) {
//        if (paxosInstances.containsKey(paxosID)) {
//          if (paxosInstances.get(paxosID).isStopped()) {
//            // paxos ID is stopped, continue
//            continue;
//          }
//          else {
//            paxosInstances.get(paxosID).startReplica();
//          }
//        }
//        else {
//          PaxosReplica paxosReplica = new PaxosReplica(paxosID,nodeID,paxosInstancesAdded.get(nodeID));
//          paxosInstances.put(paxosID,paxosReplica);
//          // TODO get initial paxos state
//          String initialPaxosState = "";
//          PaxosLogger.logPaxosStart(paxosID, paxosInstancesAdded.get(nodeID), null);
//          if(StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\tStarting replica");
//          paxosReplica.startReplica();
//        }
//      }
//    }
  }

  /**
   * Handle deletion of these paxos instances
   * @param paxosInstancesStopped
   */
  private static void handlePaxosInstanceSetStopped(ConcurrentHashMap<String, Set<Integer>> paxosInstancesStopped,
                                                    ConcurrentHashMap<String, RequestPacket> paxosInstaceStopRequests) {
    throw  new UnsupportedOperationException();
//    synchronized (paxosInstances) {
//      for (String paxosID: paxosInstancesStopped.keySet()) {
//        // paxos ID is already stopped, continue
//        if (paxosInstances.containsKey(paxosID) && paxosInstances.get(paxosID).isStopped())
//          continue;
//        PaxosReplica paxosReplica = new PaxosReplica(paxosID,nodeID,paxosInstancesStopped.get(paxosID),true, paxosInstaceStopRequests.get(paxosID));
//        paxosInstances.put(paxosID, paxosReplica);
//        PaxosLogger.logPaxosStop(paxosID);
////        PaxosLogger.logPaxosStop(paxosID, paxosInstaceStopRequests.get(paxosID));
//        clientRequestHandler.handlePaxosDecision(paxosID, paxosInstaceStopRequests.get(paxosID));
//      }
//    }
  }



  /**
   * read config file during testing/debugging
   * @param testConfig
   */
  private static void readTestConfigFile(String testConfig) {
    File f = new File(testConfig);
    if (!f.exists()) {
      if (StartNameServer.debugMode) GNS.getLogger().fine(" testConfig file does not exist. Quit. " +
              "Filename =  " + testConfig);
      System.exit(2);
    }

    try {
      BufferedReader br = new BufferedReader(new FileReader(f));
      while (true) {
        String line = br.readLine();
        if (line == null) break;
        String[] tokens = line.trim().split("\\s+");
        if (tokens.length != 2) continue;
        if (tokens[0].equals("NumberOfReplicas")) N = Integer.parseInt(tokens[1]);
        else if (tokens[0].equals("EnableLogging")) StartNameServer.debugMode = Boolean.parseBoolean(tokens[1]);
        else if (tokens[0].equals("MaxThreads")) maxThreads = Integer.parseInt(tokens[1]);
        else if (tokens[0].equals("GarbageCollectionInterval")) {
          MEMORY_GARBAGE_COLLECTION_INTERVAL = Integer.parseInt(tokens[1]);
        }

      }
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  /**
   * Create a paxos instance for testing/debugging.
   */
  private static void createDefaultPaxosInstance() {
    if (paxosInstances.containsKey(PaxosManager.getPaxosKeyFromPaxosID(defaultPaxosID))) {
      if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos instance " + defaultPaxosID + " already exists.");
      return;
    }
    // create a default paxos instance for testing.
    Set<Integer> x = new HashSet<Integer>();
    for (int i = 0;  i < N; i++)
      x.add(i);
    createPaxosInstance(defaultPaxosID, x, clientRequestHandler.getState(defaultPaxosID),0);


  }

//    /**
//     * initialize executor service during Paxos debugging/testing
//     */
//    private static void initExecutor() {
//
//        executorService = ;
//    }

  /**
   * initialize transport object during Paxos debugging/testing
   * @param configFile config file containing list of node ID, IP, port
   */
  private static NioServer initTransport(String configFile) {

    // create the worker object
    PaxosPacketDemultiplexer paxosDemux = new PaxosPacketDemultiplexer();
    ByteStreamToJSONObjects worker = new ByteStreamToJSONObjects(paxosDemux);

    // earlier worker was running as a separate thread
//        new Thread(worker).start();

    // start TCP transport thread
    NioServer tcpTransportLocal = null;
    try {
      GNS.getLogger().fine(" Node ID is " + nodeID);
      tcpTransportLocal = new NioServer(nodeID, worker, new PaxosNodeConfig(configFile));
      if (StartNameServer.debugMode) GNS.getLogger().fine(" TRANSPORT OBJECT CREATED for node  " + nodeID);
      new Thread(tcpTransportLocal).start();
    } catch (IOException e) {
      GNS.getLogger().severe(" Could not initialize TCP socket at client");
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    return tcpTransportLocal;
  }

  public static void resetAll() {
    // delete paxos instances
    paxosInstances.clear();
    // clear paxos logs
    PaxosLogger.clearLogs();
    // run java gc

  }

  /**
   * Adds a new Paxos instance to the set of actives.
   */
  public static boolean createPaxosInstance(String paxosID, Set<Integer> nodeIDs, String initialState, long initScoutDelay) {

    if(StartNameServer.debugMode) GNS.getLogger().info(paxosID + "\tEnter createPaxos");
    if (nodeIDs.size() < 3) {
      GNS.getLogger().severe(nodeID + " less than three replicas " +
              "paxos instance cannot be created. SEVERE ERROR. EXCEPTION Exception.");
      return false;
    }

    if (!nodeIDs.contains(nodeID)) {
      GNS.getLogger().severe(nodeID + " this node not a member of paxos instance. replica not created.");
      return false;
    }

//      if (paxosInstances.containsKey(PaxosManager.getPaxosKeyFromPaxosID(paxosID))) {
//        if (StartNameServer.debugMode) GNS.getLogger().info("Paxos instance already exists. Paxos ID = " + paxosID);
//        return false;
//      }

//            paxosOccupancy.put(paxosID, 1);

//    boolean preElectCoordinator = false;
    PaxosReplicaInterface r;
    // paxosInstance object can be concurrently modified.
    PaxosReplicaInterface r1;
    synchronized (paxosInstances) {
      r1 = paxosInstances.get(PaxosManager.getPaxosKeyFromPaxosID(paxosID));

      if (r1 != null && r1.getPaxosID().equals(paxosID)) {
        GNS.getLogger().warning("Paxos replica already exists .. " + paxosID);
        return false;
        //      paxosInstances.put(r1.getPaxosID(), r1);
      } else {
//        if (r1 != null && r1.getPaxosID().equals(paxosID) == false) {
////          r1.removePendingProposals();
//        }
        assert initialState != null;
        r = createPaxosReplicaObject(paxosID, nodeID, nodeIDs);//new PaxosReplicaInterface(paxosID, nodeID, nodeIDs);
        if (StartNameServer.experimentMode == false) { // During experiments, we disable state logging. This helps us load records faster into database.
          PaxosLogger.logPaxosStart(paxosID, nodeIDs, new StatePacket(r.getAcceptorBallot(), 0, initialState));
        }
        if(StartNameServer.debugMode) GNS.getLogger().info(paxosID + "\tBefore creating replica.");
        paxosInstances.put(PaxosManager.getPaxosKeyFromPaxosID(paxosID), r);
      }
    }

    if (r1 != null && r1.getPaxosID().equals(paxosID) == false) {
      r1.removePendingProposals();
      GNS.getLogger().info("OldPaxos replica replaced .. so log a stop message .. " + r1.getPaxosID() + " new replica " + paxosID);
      PaxosLogger.logPaxosStop(r1.getPaxosID());    // multiple stop msgs can get logged because other replica might stop in meanwhile.
    }

//    if(StartNameServer.debugMode) GNS.getLogger().info(paxosID + "\tStarting replica");
    if (r!= null) {
      r.checkCoordinatorFailure(); // propose new ballot if default coordinator has failed
//      if (preElectCoordinator) r.startReplicaWithPreelectedCoordinator();
//      else r.startReplica(initScoutDelay);
    }
    return true;
  }

  static PaxosReplicaInterface createPaxosReplicaObject(String paxosID, int nodeID, Set<Integer> nodeIDs1) {
//    return new PaxosReplica(paxosID, nodeID, nodeIDs1);
//    return new PaxosReplica(paxosID, nodeID, nodeIDs1);
    if (ReplicaController.isPrimaryPaxosID(paxosID)) { // Paxos among primaries uses normal paxos instances.
      return new PaxosReplica(paxosID, nodeID, nodeIDs1);
    } else {
      return new PaxosReplicaNew(paxosID, nodeID, nodeIDs1);
    }
  }

  public static void deletePaxosInstance(String paxosID) {
    throw  new UnsupportedOperationException();
//    PaxosReplica replica = paxosInstances.remove(paxosID);
//    if (replica != null)
//      PaxosLogger.logPaxosStop(paxosID);
  }

//	/**
//	 * Delete state corresponding to a Paxos instance.
//	 */
//	public static void deletePaxosInstance(String paxosID) {
//
//        PaxosReplica r = paxosInstances.remove(paxosID);
//
////        synchronized (paxosMessages) {
////            paxosMessages.remove(paxosID);
////            paxosOccupancy.remove(paxosID);
////
////        }
//		if (r != null) {
//			r.deleteState();
//			if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS INSTANCE DELETED. PaxosID = " + paxosID);
//		}
//		else {
//			if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS INSTANCE DOES NOT EXIST, DELETED ALREADY. PaxosID = " + paxosID);
//		}
//	}


//	/**
//	 * Check if the given proposed value is a stop command.
//	 * @param value command
//	 * @return true if command is a stop command, else false.
//	 */
//	static boolean isStopCommand(String value) {
//		// not a NO-OP
//		if (PaxosReplica.NO_OP.equals(value)) return false;
//		// is value == STOP, means stop command
//		if (PaxosReplica.STOP.equals(value)) return true;
//		// this code for l
//
//		if (!PaxosManager.debug) { // running as a part of GNS
//			try
//			{
//				JSONObject json = new JSONObject(value);
//				if (Packet.getPacketType(json).equals(Packet.PacketType.ACTIVE_PAXOS_STOP) ||
//						Packet.getPacketType(json).equals(Packet.PacketType.PRIMARY_PAXOS_STOP))
//					return true;
//			} catch (JSONException e)
//			{
//				if (StartNameServer.debugMode) GNS.getLogger().fine("ERROR: JSON Exception Here: " + e.getMessage());
//				e.printStackTrace();
//			}
//		}
//		return false;
//	}

  /**
   * Propose requestPacket in the paxos instance with paxosID.
   * ReqeustPacket.clientID is used to distinguish which method proposed this value.
   * @param paxosID
   * @param requestPacket
   */
  public static String propose(String paxosID, RequestPacket requestPacket) {

    if (!debug) { // running with GNS
      PaxosReplicaInterface replica = paxosInstances.get(PaxosManager.getPaxosKeyFromPaxosID(paxosID));
      if (replica == null) return null;
      try {
        replica.handleIncomingMessage(requestPacket.toJSONObject(), PaxosPacketType.REQUEST);
      } catch (JSONException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
      return replica.getPaxosID();
    }

    try
    {
      JSONObject json = requestPacket.toJSONObject();
//			if (StartNameServer.debugMode) GNRS.getLogger().fine("PAXOS PROPOSE:" + json.toString());
      // put paxos ID for identification
      json.put(PAXOS_ID, paxosID);
//			if (StartNameServer.debugMode) GNRS.getLogger().fine("PAXOS PROPOSE:" + json.toString());
      handleIncomingPacket(json);

    } catch (JSONException e)
    {
      if (StartNameServer.debugMode) GNS.getLogger().fine(" JSON Exception" + e.getMessage());
      e.printStackTrace();
    }
    return paxosID;
  }

  /**
   * check if the failure detector has reported this node as up
   * @param nodeID
   */
  public static boolean isNodeUp(int nodeID) {
    return FailureDetection.isNodeUp(nodeID);
  }


//    static int decisionCount = 0;
//    static final  Object decisionLock = new ReentrantLock();



  /**
   *
   * @param paxosID
   * @param req
   */
  static void handleDecision(String paxosID, RequestPacket req, boolean recovery) {
    clientRequestHandler.handlePaxosDecision(paxosID, req);
    if (recovery) return;
//    if (paxosInstances.containsKey(paxosID)) {
//
//    }
//    else {
//      if (StartNameServer.debugMode) GNS.getLogger().severe(nodeID + " Paxos ID not found: " + paxosID);
//    }

//    if (req.isStopRequest()) {
//      GNS.getLogger().fine("PaxosID: " + paxosID + " req: " + req + "\t" + paxosInstances);
//      synchronized (paxosInstances) {
//        PaxosReplicaInterface r = paxosInstances.get(PaxosManager.getPaxosKeyFromPaxosID(paxosID));
//        if (r == null) return;
//        if (r.getPaxosID().equals(paxosID)) {
//          if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos instance removed " + paxosID  + "\tReq " + req);
//          paxosInstances.remove(PaxosManager.getPaxosKeyFromPaxosID(paxosID));
////          r.logFullResponseAfterStop();
//        } else {
//          if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos instance already removed " + paxosID);
//        }
//      }
//    }
  }

  /**
   * If a node fails, or comes up again, the respective Paxos instances are informed.
   * Some of them may elect a new co-ordinator.
   */
  static void informNodeStatus(FailureDetectionPacket fdPacket) {
    GNS.getLogger().info("Handling node failure = " + fdPacket.responderNodeID);
    for (String x: paxosInstances.keySet()) {
      PaxosReplicaInterface r = paxosInstances.get(x);

      if (r.isNodeInPaxosInstance(fdPacket.responderNodeID)) {
        try
        {
          r.handleIncomingMessage(fdPacket.toJSONObject(), fdPacket.packetType);
//          JSONObject json = ;
//          json.put(PAXOS_ID, x);
//          processMessage(new HandlePaxosMessageTask(json,fdPacket.packetType));
        } catch (JSONException e)
        {
          if (StartNameServer.debugMode) GNS.getLogger().fine(" JSON Exception");
          e.printStackTrace();
        }
      }
    }
    // inform output handler of node failure
    clientRequestHandler.handleFailureMessage(fdPacket);
  }

  /**
   * Handle incoming message, incoming message could be of any Paxos instance.
   * @param json
   */
  public static void handleIncomingPacket(JSONObject json) throws JSONException {
//    long t0 = System.currentTimeMillis();
//    long tA = 0;
//    long tB = 0;
//    long tC = 0;
//    long tD = 0;
//    long tE = 0;
//    long tF = 0;
    int incomingPacketType;
    try {
//                    tA = System.currentTimeMillis();
//      if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos manager recvd msg: " + json.toString());
//      tB = System.currentTimeMillis();
      incomingPacketType = json.getInt(PaxosPacketType.ptype);
    } catch (JSONException e) {
      e.printStackTrace();
      return;
    }
    switch (incomingPacketType){
//      case PaxosPacketType.ACCEPT_REPLY:
//        AcceptReplyPacket accept = new AcceptReplyPacket(json);
//        GNS.getLogger().severe(accept.ballot.toString());
//        break;
      case PaxosPacketType.DECISION:
//            case PaxosPacketType.ACCEPT:
//            case PaxosPacketType.PREPARE:
//        tC = System.currentTimeMillis();
        try {
          PaxosLogger.logMessage(new LoggingCommand(json.getString(PAXOS_ID), json, LoggingCommand.LOG_AND_EXECUTE));
        } catch (JSONException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
//        tD = System.currentTimeMillis();
        break;
      case PaxosPacketType.FAILURE_DETECT:
      case PaxosPacketType.FAILURE_RESPONSE:
        processMessage(new HandleFailureDetectionPacketTask(json));
        break;
      default:
        processMessage(new HandlePaxosMessageTask(json,incomingPacketType));
    }
//    long t1 = System.currentTimeMillis();
//
//    if (t1 - t0 > 10) {
//      GNS.getLogger().severe("Long latency Paxos Manager " + (t1 - t0) + " Breakdown: " + (tA - t0) + "\t" + (tB - tA) + "\t" + (tC - tB) + "\t" + (tD - tC) + "\t" + (t1 - tD));
//    }

//        if (json.has(PAXOS_ID)) {
////            if (StartNameServer.debugMode) GNS.getLogger().fine(paxosid + "\tPAXOS RECVD MSG: " + json);
//
//
//            if (incomingPacketType == PaxosPacketType.DECISION || incomingPacketType == PaxosPacketType.ACCEPT) {
//
//
//            } else {
//
//            }
//        }
//        else { // Failure detection packet
//
//        }
  }
  static Random r = new Random();
  static void processMessage(Runnable runnable) {
//    if (r.nextDouble() < 0.02) GNS.getLogger().severe("Submitted message to queue");
    if (executorService!= null) executorService.submit(runnable);
  }

  static void sendMessage(int destID, JSONObject json, String paxosID) {
    try {
      json.put(PaxosManager.PAXOS_ID, paxosID);
      sendMessage(destID, json);
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

  }

  static void sendMessage(Set<Integer> destIDs, JSONObject json, String paxosID) {
    try {
      json.put(PaxosManager.PAXOS_ID, paxosID);
      sendMessage(destIDs, json);
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

  }

  static void sendMessage(short[] destIDs, JSONObject json, String paxosID, int excludeID) {
    try {
      json.put(PaxosManager.PAXOS_ID, paxosID);
      sendMessage(destIDs, json, excludeID);
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

  }

  static void sendMessage(short[] destIDs, JSONObject json, int excludeID) {
    try {
      if (!debug) {
        Packet.putPacketType(json, Packet.PacketType.PAXOS_PACKET);
      }
      tcpTransport.sendToIDs(destIDs, json, excludeID);
    } catch (IOException e)
    {
      GNS.getLogger().severe("Paxos: IO Exception in sending to IDs. " + destIDs);
    } catch (JSONException e)
    {
      GNS.getLogger().severe("JSON Exception in sending to IDs. " + destIDs);
    }
  }

  /**
   * all paxos instances use this method to exchange messages
   * @param destID
   * @param json
   */
  static void sendMessage(int destID, JSONObject json) {
    try
    {
      if (!debug) {
        Packet.putPacketType(json, Packet.PacketType.PAXOS_PACKET);

      }
//      GNS.getLogger().fine("Sending message to " + destID + "\t" + json);
      tcpTransport.sendToID(destID, json);
    } catch (IOException e)
    {
      GNS.getLogger().severe("Paxos: IO Exception in sending to ID. " + destID);
    } catch (JSONException e)
    {
      GNS.getLogger().severe("JSON Exception in sending to ID. " + destID);
    }

  }

  static void sendMessage(Set<Integer> destIDs, JSONObject json) {
    try {
      if (!debug) {
        Packet.putPacketType(json, Packet.PacketType.PAXOS_PACKET);
      }
      tcpTransport.sendToIDs(destIDs, json);
    } catch (IOException e)
    {
      GNS.getLogger().severe("Paxos: IO Exception in sending to IDs. " + destIDs);
    } catch (JSONException e)
    {
      GNS.getLogger().severe("JSON Exception in sending to IDs. " + destIDs);
    }
  }


  static void addToActiveProposals(ProposalStateAtCoordinator propState) {
    synchronized (proposalStates) {
      proposalStates.add(propState);
    }
  }

  static void removeFromActiveProposals(ProposalStateAtCoordinator propState) {
    synchronized (proposalStates){
      proposalStates.remove(propState);
    }
  }

  public static String LOG_MSG = "LOGMSG";
  private static int logMsgID = 0;
  private static Lock logMsgIDLock = new ReentrantLock();

  /**
   * this methods does paxos logging in mongo DB.
   * We prefer to write directly to disk as it gives better performance.
   * @param jsonObject
   * @param paxosID
   */
  static void addToPaxosLog(JSONObject jsonObject, String paxosID) {
    int msgID;
    try {
      logMsgIDLock.lock();
      msgID = ++logMsgID;

    } finally {
      logMsgIDLock.unlock();
    }
    try {
      jsonObject.put(PAXOS_ID,paxosID);
      jsonObject.put(LOG_MSG,msgID);
      MongoRecords.getInstance().insert(MongoRecords.PAXOSLOG, "PaxosLog", jsonObject);
    } catch (JSONException e) {
      e.printStackTrace();
    } catch (RecordExistsException e) {
      // Should not happen because record has no unique key.
      e.printStackTrace();
    }

  }

  public static String getPaxosKeyFromPaxosID(String paxosID) {
    if (ReplicaController.isPrimaryPaxosID(paxosID)) return paxosID; // paxos between primaries
    else { // paxos between actives.
      int index = paxosID.lastIndexOf("-");
      if (index == -1) return paxosID;
      return paxosID.substring(0, index);
    }
  }

  /**
   * main funtion to test the paxos manager code.
   * @param args
   */
  public static void main(String[] args) {
    if (args.length != 4) {
      System.out.println("QUIT. Incorrect arguments.\nUsage: PaxosManager <NumberOfPaoxsNodes> <ReplicaID> <NodeConfigFile>");
      System.exit(2);
    }
    // node IDs (for paxos replicas) = 0, 1, ..., N - 1
    String nodeConfigFile = args[0];
    String testConfig = args[1];
    String paxosLogFolder = args[2];
    int myID = Integer.parseInt(args[3]);
    PaxosManager.setPaxosLogFolder(paxosLogFolder + "/paxoslog_" + myID);
    initializePaxosManagerDebugMode(nodeConfigFile, testConfig, myID, new DefaultPaxosInterface());
  }
}


class PaxosPacketDemultiplexer extends PacketDemultiplexer {

  @Override
  public void handleJSONObjects(ArrayList jsonObjects) {
    for (Object j: jsonObjects) {
//            try {
      JSONObject json = (JSONObject)j;
      try {
        PaxosManager.handleIncomingPacket(json);
      } catch (JSONException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
//                int incomingPacketType = json.getInt(PaxosPacketType.ptype);
//                if (incomingPacketType == PaxosPacketType.REMOVE) {
//                    String paxosID = json.getString(PaxosManager.PAXOS_ID);
//                    RequestPacket req = new RequestPacket(json);
//                    PaxosManager.clientRequestHandler.proposeRequestToPaxos(paxosID, req);
//                }
//                else {
//                    PaxosManager.handleIncomingPacket(json);
//                }
//            } catch (JSONException e) {
//                if (StartNameServer.debugMode) GNS.getLogger().fine("JSON Exception: PaxosPacketType not found");
//                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//                continue;
//            }

    }

  }
}


class HandlePaxosMessageTask extends TimerTask {

  JSONObject json;

  int packetType;


  HandlePaxosMessageTask(JSONObject json, int packetType){
    this.json = json;
    this.packetType = packetType;
  }

  @Override
  public void run() {

    long t0 = System.currentTimeMillis();
    try {
      String paxosID;
      try {
        paxosID = json.getString(PaxosManager.PAXOS_ID);
      } catch (JSONException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        return;
      }
//      GNS.getLogger().severe("StartP:" + paxosID);
      PaxosReplicaInterface replica = PaxosManager.paxosInstances.get(PaxosManager.getPaxosKeyFromPaxosID(paxosID));
      if (replica != null && replica.getPaxosID().equals(paxosID)) {
//                if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\tPAXOS PROCESS START " + paxosID + "\t" +  json);
        replica.handleIncomingMessage(json,packetType);

//                if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\tPAXOS MSG DONE " + paxosID + "\t" +  json);
      }
      else {
        // this case can arise just after a paxos instance is created or stopped.
        GNS.getLogger().warning("ERROR: Paxos Instances does not contain ID = " + paxosID);
      }
    } catch (Exception e) {
      GNS.getLogger().severe(" PAXOS Exception EXCEPTION!!. Msg = " + json);
      e.printStackTrace();
    }
    long t1 = System.currentTimeMillis();
    if (t1 - t0 > 100)
      GNS.getLogger().severe("Long delay " + (t1 - t0) + "ms. Packet: " + json);
  }
}

/**
 * Resend proposals (for all paxos instances) that have not yet been accepted by majority.
 */
class ResendPendingMessagesTask extends TimerTask{

//  @Override
//  public  void run() {
//    for (String x:PaxosManager.paxosInstances.keySet()) {
//      PaxosReplica paxosReplica = PaxosManager.paxosInstances.get(x);
//      if (paxosReplica !=null) {
//        paxosReplica.resendPendingAccepts();
//        paxosReplica.checkIfReplicasUptoDate();
//      }
//    }
//  }

  @Override
  public void run() {

    ArrayList<ProposalStateAtCoordinator> reattempts = new ArrayList<ProposalStateAtCoordinator>();

    try{
      // synchronization over
      synchronized (PaxosManager.proposalStates){
        for (ProposalStateAtCoordinator propState: PaxosManager.proposalStates) {
          if (propState.getTimeSinceAccept() > PaxosManager.RESEND_PENDING_MSG_INTERVAL_MILLIS) {
            reattempts.add(propState);
          }
          else break;
        }
      }

      for (ProposalStateAtCoordinator propState: reattempts) {
        boolean result = propState.paxosReplica.resendPendingProposal(propState);
        if (result == false) {
          synchronized (PaxosManager.proposalStates){
            PaxosManager.proposalStates.remove(propState);
          }
        }

        if (StartNameServer.experimentMode) GNS.getLogger().severe("\tResendingMessage\t" +
                propState.paxosReplica.getPaxosID() + "\t" + propState.pValuePacket.proposal.slot + "\t" + result + "\t");

      }

      PaxosManager.startResendPendingMessages();

    }catch (Exception e) {
      GNS.getLogger().severe("Exception in sending pending messages." + e.getMessage());
      e.printStackTrace();
    }


    //        if (propState.paxosReplica.getClass().equals(PaxosReplicaNew.class)) {
    //          JSONObject json = new JSONObject();
    //          json.put(PaxosManager.PAXOS_ID, propState.paxosReplica.getPaxosID());
    //          json.put("slot", propState.pValuePacket.proposal.slot);
    //          PaxosManager.executorService.submit(new HandlePaxosMessageTask(json,PaxosPacketType.RESEND_ACCEPT));
    //        }

    //    for (String x:PaxosManager.paxosInstances.keySet()) {
//      PaxosReplica paxosReplica = PaxosManager.paxosInstances.get(x);
//      if (paxosReplica !=null) {
//        paxosReplica.resendPendingAccepts();
//        paxosReplica.checkIfReplicasUptoDate();
//      }
//    }
  }
}


class CheckPaxosRecoveryCompleteTask extends TimerTask {

  @Override
  public void run() {
    PaxosManager.checkLogSynchronizationOver();
  }
}

/**
 * periodically logs state of all paxos instances
 */
class LogPaxosStateTask extends TimerTask {

  @Override
  public void run() {
    try {

      if (StartNameServer.experimentMode) {return;} // we do not log paxos state during experiments ..

      GNS.getLogger().info("Logging paxos state task.");
      for (String paxosKey: PaxosManager.paxosInstances.keySet()) {

        PaxosReplicaInterface replica = PaxosManager.paxosInstances.get(paxosKey);
        if (paxosKey != null) {
          StatePacket packet = replica.getState();
          if (packet != null) {
            PaxosLogger.logPaxosState(replica.getPaxosID(), packet);
          }
        }
      }
      GNS.getLogger().info("Completed logging.");
    }catch(Exception e) {
      GNS.getLogger().severe("Exception IN paxos state logging " + e.getMessage());
      e.printStackTrace();
    }
  }

}