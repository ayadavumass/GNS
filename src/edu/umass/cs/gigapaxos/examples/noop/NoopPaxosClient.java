package edu.umass.cs.gigapaxos.examples.noop;

import java.io.IOException;

import org.json.JSONException;

import edu.umass.cs.gigapaxos.InterfaceClientRequest;
import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.gigapaxos.PaxosClientAsync;
import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.RequestCallback;

/**
 * @author arun
 * 
 *         A simple client for NoopApp.
 */
public class NoopPaxosClient extends PaxosClientAsync {

	/**
	 * @throws IOException
	 */
	public NoopPaxosClient() throws IOException {
		super();
	}

	/**
	 * A simple example of asynchronously sending a few requests with a callback
	 * method that is invoked when the request has been executed or is known to
	 * have failed.
	 * 
	 * @param args
	 * @throws IOException
	 * @throws JSONException
	 */
	public static void main(String[] args) throws IOException, JSONException {
		NoopPaxosClient noopClient = new NoopPaxosClient();
		for (int i = 0; i < 3; i++) {
			final String requestValue = "hello world" + i;
			noopClient.sendRequest(PaxosConfig.application.getSimpleName()+"0",
					requestValue, new RequestCallback() {

						@Override
						public void handleResponse(InterfaceRequest response) {
							System.out
									.println("Response for request ["
											+ requestValue
											+ "] = "
											+ (response instanceof InterfaceClientRequest ? ((InterfaceClientRequest) response)
													.getResponse() : null));
						}
					});
		}
	}
}
