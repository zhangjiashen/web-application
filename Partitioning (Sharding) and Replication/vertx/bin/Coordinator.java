import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.Iterator;
import java.util.Collections;
import java.util.List;
import java.sql.Timestamp;
import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class Coordinator extends Verticle {

	//Default mode: replication. Possible string values are "replication" and "sharding"
	private static String storageType = "replication";

	/**
	 * TODO: Set the values of the following variables to the DNS names of your
	 * three dataCenter instances
	 */
	private static final String dataCenter1 = "<DNS-OF-DATACENTER-1>";
	private static final String dataCenter2 = "<DNS-OF-DATACENTER-2>";
	private static final String dataCenter3 = "<DNS-OF-DATACENTER-3>";

	private static int getcnt = 0;
	private static Semaphore enqueue = new Semaphore(1,true);
	private static Semaphore putlock = new Semaphore(1,false);
	private static Semaphore getcntlock = new Semaphore(1,false);
	
	
	private class RepKeyVal{
		//public String key;
		public String val;
		boolean putget;//true for put, false for get
		public RepKeyVal(String val, boolean b){
			//this.key = key;
			this.val = val;
			this.putget = b;
		}
	}
	
	//public static HashMap<String, Queue<RepKeyVal>> queuemap = new HashMap<String, Queue<RepKeyVal>> ();
	
	private static HashMap<String, Boolean> putlockmap = new HashMap<String, Boolean> ();
	private static HashMap<String, Integer> getcntmap = new HashMap<String, Integer> ();
	private static HashMap<String, Boolean> getcntlockmap = new HashMap<String, Boolean> ();
	
	private static BlockingQueue<repKeyVal> repBlockingQueue = new BlockingQueue<repKeyVal> ();
	
	
	@Override
	public void start() {
		//DO NOT MODIFY THIS
		KeyValueLib.dataCenters.put(dataCenter1, 1);
		KeyValueLib.dataCenters.put(dataCenter2, 2);
		KeyValueLib.dataCenters.put(dataCenter3, 3);
		final RouteMatcher routeMatcher = new RouteMatcher();
		final HttpServer server = vertx.createHttpServer();
		server.setAcceptBacklog(32767);
		server.setUsePooledBuffers(true);
		server.setReceiveBufferSize(4 * 1024);

		routeMatcher.get("/put", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String value = map.get("value");
				//You may use the following timestamp for ordering requests
				
				/* timestamp and enqueue */
				enqueue.acquire();
                                final String = new Timestamp(System.currentTimeMillis() 
                                                             + TimeZone.getTimeZone("EST").getRawOffset()).toString();
                //repBlockingQueue.put(new RepKeyVal(value, true));
               Queue<RepKeyVal> tmpqueue = new Queue<RepKeyVal>();
                	
                if (queuemap.containsKey(key)){
                	queuemap.get(key).add(new RepKeyVal(value, true));
                }else {
                	
                }
                enqueue.release();
                
				Thread t = new Thread(new Runnable() {
					public void run() {
						
						safePutGet();
						//TODO: Write code for PUT operation here.
						//Each PUT operation is handled in a different thread.
						//Highly recommended that you make use of helper functions.
						//KeyValueLib.dataCenters.put(dataCenter1, key, value);
						//KeyValueLib.dataCenters.put(dataCenter2, key, value);
						//KeyValueLib.dataCenters.put(dataCenter3, key, value);
						
					}
				});
				t.start();
				req.response().end(); //Do not remove this
			}
		});

		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String loc = map.get("loc");
				//You may use the following timestamp for ordering requests
				final String timestamp = new Timestamp(System.currentTimeMillis() 
								+ TimeZone.getTimeZone("EST").getRawOffset()).toString();
				Thread t = new Thread(new Runnable() {
					public void run() {
						//TODO: Write code for GET operation here.
                                                //Each GET operation is handled in a different thread.
                                                //Highly recommended that you make use of helper functions.
						req.response().end("0"); //Default response = 0
					}
				});
				t.start();
			}
		});

		routeMatcher.get("/storage", new Handler<HttpServerRequest>() {
                        @Override
                        public void handle(final HttpServerRequest req) {
                                MultiMap map = req.params();
                                storageType = map.get("storage");
                                //This endpoint will be used by the auto-grader to set the 
				//consistency type that your key-value store has to support.
                                //You can initialize/re-initialize the required data structures here
                                
                                req.response().end();
                        }
                });

		routeMatcher.noMatch(new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				req.response().putHeader("Content-Type", "text/html");
				String response = "Not found.";
				req.response().putHeader("Content-Length",
						String.valueOf(response.length()));
				req.response().end(response);
				req.response().close();
			}
		});
		server.requestHandler(routeMatcher);
		server.listen(8080);
	}
	
	
	/* Put and Get
	 * reference:
	 * https://en.wikipedia.org/wiki/Readers%E2%80%93writers_problem
	 *  */
	public void safePutGet(){
		
		RepKeyVal repKeyVal = repBlockingQueue.take();
		if (repKeyVal.putget){
			
			
			//putlock.acquire();
			if (putlockmap.containsKey(repKeyVal.key){
				
				
			}
			putlockmap.get(repKeyVal.key);
			KeyValueLib.dataCenters.put(dataCenter1, repKeyVal.key, repKeyVal.val);
			KeyValueLib.dataCenters.put(dataCenter2, repKeyVal.key, repKeyVal.val);
			KeyValueLib.dataCenters.put(dataCenter3, repKeyVal.key, repKeyVal.val);
			
			putlock.release();
		}
	}
}
