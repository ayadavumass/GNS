package edu.umass.cs.gnsserver.gnsapp.nonblockingselect.helper;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.NSSelectInfo;
import edu.umass.cs.gnsserver.gnsapp.nonblockingselect.SelectFuture;

/**
 * Stores the information about pending select requests.
 * This class also implements SeelctFuture.
 * @author ayadav
 *
 */
public class PendingSelectReqInfo implements SelectFuture
{
	private final CommandPacket originalRequest;
	private final long reqArrivalTime;
	private final NSSelectInfo nsInfo;
	private boolean completed;
	
	public PendingSelectReqInfo(CommandPacket originalRequest, NSSelectInfo nsInfo)
	{
		this.originalRequest = originalRequest;
		this.reqArrivalTime = System.currentTimeMillis();
		this.nsInfo = nsInfo;
		completed = false;
	}
	
	/**
	 * This constructor is used for erroneous select requests, like
	 * bad signature etc. 
	 * @param completed
	 */
	public PendingSelectReqInfo(boolean completed)
	{
		this.completed = completed;
		originalRequest = null;
		this.reqArrivalTime = System.currentTimeMillis();
		nsInfo = null;
	}
	
	public CommandPacket getOriginalRequest()
	{
		return this.originalRequest;
	}
	
	public long getReqArrivalTime()
	{
		return this.reqArrivalTime;
	}
	
	public NSSelectInfo getNSInfo()
	{
		return this.nsInfo;
	}
	
	public void setCompletion()
	{
		completed = true;
		synchronized(this)
		{
			// specifically using notifyAll instead of notify
			// as many thread could be waiting on the Future.get()
			this.notifyAll();
		}
	}
	
	
	public boolean getCompletition()
	{
		return completed;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) 
	{
		throw new RuntimeException("Cancel of CNS select operation is not supported.");
		//return false;
	}
	
	@Override
	public Boolean get() throws InterruptedException, ExecutionException 
	{
		synchronized(this)
		{
			while(!completed)
			{
				this.wait();
			}
		}
		return true;
	}
	
	@Override
	public Boolean get(long timeout, TimeUnit unit) 
							throws InterruptedException, ExecutionException, TimeoutException 
	{
		synchronized(this)
		{
			while(!completed)
			{
				this.wait(unit.toMillis(timeout));
			}
		}
		
		return true;
	}

	@Override
	public boolean isCancelled() 
	{
		throw new RuntimeException("Cancel of CNS select operation is not supported.");
	}

	@Override
	public boolean isDone() 
	{
		return this.completed;
	}
}