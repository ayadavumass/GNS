/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.gnsapp.packet;

/**
 * The select operation.
 */
public enum SelectOperation {
  /**
   * Special case query for field with value.
   */
  EQUALS, /**
   * Special case query for location field near point.
   */
  NEAR, /**
   * Special case query for location field within bounding box.
   */
  WITHIN, /**
   * General purpose query.
   */
  QUERY,
  
  /**
   * The case when a notification is also sent to GUIDs that satisfy a query in a select request.
   */
  SELECT_NOTIFY,
  
  /**
   * The case for querying a notification status from an entry-point name servers to 
   * other name servers.
   */
  NOTIFICATION_STATUS,
  
  /**
   * The notification status reply from name servers to an entry-point name server.
   */
  NOTIFICATION_STATUS_REPLY,
}