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
package edu.umass.cs.gnsclient.client.singletests;

import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;
import org.junit.Assert;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * This test insures that a simple acl add gives field access to another guid.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AclAllFieldsSuperuser extends DefaultGNSTest {
	
  private static GuidEntry masterGuid;

  /**
   *
   */
  public AclAllFieldsSuperuser() {
  }

  private static GuidEntry barneyEntry;

  /**
   *
   */
  @Test
  public void test_100_LookupMasterGuid() {
    try {
      masterGuid = GuidUtils.getGUIDKeys(globalAccountName);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception while creating guid: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_142_ACLCreateAnotherGuid() {
    try {
      String barneyName = "barney" + RandomString.randomString(12);
      try {
    	  client.execute(GNSCommand.lookupGUID(barneyName));
        Utils.failWithStackTrace(barneyName + " entity should not exist");
      } catch (ClientException e) {
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception looking up Barney: ", e);
      }
      client.execute(GNSCommand.guidCreate(masterGuid, barneyName));
      barneyEntry = GuidUtils.lookupGuidEntryFromDatabase(client, barneyName);
      try {
        client.execute(GNSCommand.lookupGUID(barneyName));
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception looking up Barney: ", e);
      }
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it in ACLPartTwo: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_143_ACLCreateFields() {
    try {
      // remove default read access for this test
      client.execute(GNSCommand.fieldUpdate(barneyEntry.getGuid(), "cell", "413-555-1234", barneyEntry));
      client.execute(GNSCommand.fieldUpdate(barneyEntry.getGuid(), "address", "100 Main Street", barneyEntry));
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it in ACLPartTwo: ", e);
    }
  }

  private static GuidEntry superuserEntry;

  /**
   *
   */
  @Test
  public void test_144_ACLCreateSuperUser() {
    String superUserName = "superuser" + RandomString.randomString(12);
    try {
      try {
    	  client.execute(GNSCommand.lookupGUID(superUserName));
        Utils.failWithStackTrace(superUserName + " entity should not exist");
      } catch (ClientException e) {
      }
      client.execute(GNSCommand.guidCreate(masterGuid, superUserName));
      superuserEntry =GuidUtils.lookupGuidEntryFromDatabase(client, superUserName);
      try {
        Assert.assertEquals(superuserEntry.getGuid(), 
        		client.execute(GNSCommand.lookupGUID(superUserName)).getResultString());
        		
        		//clientCommands.lookupGuid(superUserName));
      } catch (ClientException e) {
      }
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it in ACLALLFields: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_145_ACLAddALLFields() {
    try {
      // let superuser read any of barney's fields
      client.execute(GNSCommand.aclAdd(AclAccessType.READ_WHITELIST, barneyEntry,
              GNSProtocol.ENTIRE_RECORD.toString(), superuserEntry.getGuid()));
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it in ACLALLFields: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_146_ACLTestAllFieldsSuperuser() {
    try {
      Assert.assertEquals("413-555-1234",
    		  client.execute(GNSCommand.fieldRead(barneyEntry.getGuid(), "cell", superuserEntry)));
      Assert.assertEquals("100 Main Street",
      		  client.execute(GNSCommand.fieldRead(barneyEntry.getGuid(), "address", superuserEntry)));

    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it in ACLALLFields: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_147_ACLTestAllFieldsSuperuserCleanup() {
    try {
      client.execute(GNSCommand.guidRemove(masterGuid, superuserEntry.getGuid()));
      client.execute(GNSCommand.guidRemove(masterGuid, superuserEntry.getGuid()));
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception during cleanup: " + e);
    }
  }

}
