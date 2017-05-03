package edu.umass.cs.gnsserver.gnsapp.cns.selectpolicy;

import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectRequestPacket;


/**
 * This class is an abstract class for specifying the select request
 * processing policy in the GNS. 
 * 
 * The object of this class is created based on Java reflection, 
 * by taking the class name as input from the GNS config files. The reflection class creation
 * has no requirements for any specific constructors that the child classes should implement.
 * 
 * @author ayadav
 */
public abstract class AbstractSelectPolicy
{
	protected static final Logger LOG = Logger.getLogger(AbstractSelectPolicy.class.getName());
	
	/**
	 * This function returns the a list of nodes, in terms of socket addresses, on which the GNS should 
	 * process the select request. The returned set of nodes depends on the implementation of this class. 
	 * For example, the returned set could be all nodes or any subset of nodes.
	 * 
	 * The function returns Set<InetSocketAddress> instead of Set<NodeIDType> because the node id to socket address
	 * mapping could have changed by the time the GNS tries to map node ids to socket addresses. 
	 * So, the function makes a copy of the node id to socket address mapping in the beginning of the function 
	 * call and returns the result based on that mapping.
	 * 
	 * @param selectreq
	 * @return
	 */
	public abstract Set<InetSocketAddress> getNodesForSelectRequest(SelectRequestPacket selectreq);
	
	
	/**
	 * Creates the object of clazz by reflection. clazz will be a child class of
	 * AbstractSelectPolicy whose class path will be specified in the GNS config files.
	 * @param clazz
	 * @return
	 */
	public static AbstractSelectPolicy createSelectPolicy(Class<?> clazz) 
	{
		try 
		{
			return (AbstractSelectPolicy) clazz.getConstructor().newInstance();
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			LOG.log(Level.SEVERE, 
					e.getClass().getSimpleName() + " while creating " + clazz);
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * Fetches the current set of actives. Currently it fetches by reading the gns config files, but 
	 * a TODO: is to directly fetch these from reconfigurators, which is a good design. 
	 * @return
	 */
	protected Set<InetSocketAddress> fetchCurrentActives()
	{
		return new HashSet<>(PaxosConfig.getActives().values());
	}
}