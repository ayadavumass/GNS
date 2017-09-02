package edu.umass.cs.gnsserver.gnsapp.selectnotification;

import java.util.List;


/**
 * This interface defines the methods for sending the notifications 
 * from the GNS for a select request. 
 * 
 * @author ayadav
 *
 */
public interface SendNotification 
{
	/**
	 * This method is used to implement the logic to send {@code notificationStr}
	 * to {@code guidList}. 
	 * 
	 * @param guidList
	 * @param notificationStr
	 * @return
	 * An object of {@link NotificationSendingStats} 
	 * to keep track of notification sending progress.
	 */
	public NotificationSendingStats sendNotification(List<String> guidList, String notificationStr);
}