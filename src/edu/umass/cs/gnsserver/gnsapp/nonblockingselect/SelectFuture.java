package edu.umass.cs.gnsserver.gnsapp.nonblockingselect;

import java.util.concurrent.Future;

/**
 * 
 * The select future interface for the non blocking select calls.
 * get() method returns true if the request handling was successful. 
 * @author ayadav
 */
public interface SelectFuture extends Future<Boolean>
{
}