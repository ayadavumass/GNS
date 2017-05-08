package edu.umass.cs.gnsserver.gnsapp.nonblockingselect;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Implements the select future for the non blocking select calls.
 * get() method returns true if the request handling was successful. 
 * @author ayadav
 *
 */
public class SelectFuture implements Future<Boolean>
{

	@Override
	public boolean cancel(boolean arg0) 
	{	
		return false;
	}

	@Override
	public Boolean get() throws InterruptedException, ExecutionException 
	{
		return null;
	}

	@Override
	public Boolean get(long arg0, TimeUnit arg1) throws InterruptedException, ExecutionException, TimeoutException 
	{	
		return null;
	}

	@Override
	public boolean isCancelled() 
	{	
		return false;
	}

	@Override
	public boolean isDone() 
	{	
		return false;
	}
}