import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.Queue;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class KeyValueStore extends Verticle {
	// store key-value lists
	private static HashMap<String, ArrayList<StoreValue>> store = null;
	// A queue to store the timestamps of all received PUT 
	public static HashMap<String, PriorityBlockingQueue<Long>> putTime = new HashMap<String, PriorityBlockingQueue<Long>>();
	// to protect each key's list
	public static HashMap<String, Semaphore> semStore = new HashMap<String, Semaphore> ();
	// to protect each key's queue
	//public static HashMap<String, Semaphore> semQueue = new HashMap<String, Semaphore> ();
	
	// to protect hashmap PutTime
	public static Semaphore mutexPutTime = new Semaphore(1,false);
	// to protect hashmap store
	public static Semaphore mutexStore = new Semaphore(1,false);
	// to protect hashmap semStore
	public static Semaphore mutexMapSemStore = new Semaphore(1,false);
	// to protect hashmap semQueue
	//public static Semaphore mutexMapSemQueue = new Semaphore(1,false);
	
	
	public KeyValueStore() {
		store = new HashMap<String, ArrayList<StoreValue>>();
	}

	@Override
	public void start() {
		final KeyValueStore keyValueStore = new KeyValueStore();
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
				final String consistency = map.get("consistency");
				final Integer region = Integer.parseInt(map.get("region"));

				final Long timestamp = Long.parseLong(map.get("timestamp"));
				final Long adjustedtimestamp = Skews.handleSkew(timestamp, region);;
				
				Thread t = new Thread(new Runnable() {
					public void run() {
						switch (consistency) {
						case "strong":
							DCputStrong(key, value, adjustedtimestamp);
							break;
						case "causal":
							DCputCausal(key, value, adjustedtimestamp);
							break;
						case "eventual":
							DCputEventual(key, value, adjustedtimestamp);
							break;
						default:
							System.out.println("Unknown consistency type:" + consistency);
							
						}
						
						String response = "stored";
						req.response().putHeader("Content-Type", "text/plain");
						req.response().putHeader("Content-Length",
								String.valueOf(response.length()));
						req.response().end(response);
						req.response().close();
					}
				});
				t.start();
				
			}
		});
		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String consistency = map.get("consistency");
				final Long timestamp = Long.parseLong(map.get("timestamp"));

				System.out.println("GET:" + key + " timestamp:" + timestamp);
				
				
				
				Thread t = new Thread(new Runnable() {
					public void run() {
						String response = null;
						switch (consistency) {
						case "strong":
							response = DCgetStrong(key, timestamp);
							break;
						case "causal":
							response = DCgetCausal(key, timestamp);
							break;
						case "eventual":
							response = DCgetEventual(key, timestamp);
							break;
						default:
							System.out.println("unknown consistency type: " + consistency);
						}
						
						req.response().putHeader("Content-Type", "text/plain");
						if (response != null)
							req.response().putHeader("Content-Length",
									String.valueOf(response.length()));
						req.response().end(response);
						req.response().close();
					}
				});
				t.start();
				
			}
		});
		// Handler for when the AHEAD is called
		routeMatcher.get("/ahead", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				String key = map.get("key");
				final Long timestamp = Long.parseLong(map.get("timestamp"));
				System.out.println("ahead" + timestamp);
				/* add time threshold of put into the queue of the key, any GET after the threshold should be blocked */
				PriorityBlockingQueue<Long> tmpqueue = null;
				try {
					mutexPutTime.acquire();
				} catch (InterruptedException e) {}
				if (putTime.containsKey(key)){
					tmpqueue = putTime.get(key);
				}else{
					tmpqueue = new PriorityBlockingQueue<Long>();
					putTime.put(key, tmpqueue);
				}
				tmpqueue.add(timestamp);
				mutexPutTime.release();
				
				//acquireQueueSem(key);
				//thread-safe
				//releaseQueueSem(key);
				
				req.response().putHeader("Content-Type", "text/plain");
				req.response().end();
				req.response().close();
			}
		});
		// Handler for when the COMPLETE is called
		routeMatcher.get("/complete", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				String key = map.get("key");
				final long timestamp = Long.parseLong(map.get("timestamp"));
				PriorityBlockingQueue<Long> tmpqueue = null;
				try {
					mutexPutTime.acquire();
				} catch (InterruptedException e) {}
				tmpqueue = putTime.get(key);
				mutexPutTime.release();
				
				System.out.println("complete");
				
				long removeTS = tmpqueue.remove();//thread-safe
				if(removeTS != timestamp){
					System.out.println("Datacenter time threshold queue disorder! RemoveTS:" + removeTS + "timestamp:" + timestamp);
				}
				
				
				req.response().putHeader("Content-Type", "text/plain");
				req.response().end();
				req.response().close();
			}
		});
		// Clears this stored keys. Do not change this
		routeMatcher.get("/reset", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				System.out.println("*************************Reset***************************");
				KeyValueStore.store.clear();
				putTime.clear();
				semStore.clear();
				// to protect hashmap PutTime
				mutexPutTime = new Semaphore(1,false);
				// to protect hashmap store
				mutexStore = new Semaphore(1,false);
				// to protect hashmap semStore
				mutexMapSemStore = new Semaphore(1,false);
				req.response().putHeader("Content-Type", "text/plain");
				req.response().end();
				req.response().close();
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
	
	void DCputEventual(String key, String value, long adjustedtimestamp){
		ArrayList<StoreValue> tmplist;
		
		System.out.println("PUT:" + key + " Value:" + value + " timestamp:" + adjustedtimestamp);
		/* get store list */
		try {
			mutexStore.acquire();
		} catch (InterruptedException e) {}
		if (store.containsKey(key)){
			tmplist = store.get(key);
		} else{
			tmplist = new ArrayList<StoreValue>();
			store.put(key, tmplist);
		}
		mutexStore.release();
		
		StoreValue sv = new StoreValue(adjustedtimestamp, value);
		
		acquireKeySem(key);
		tmplist.add(sv);
		releaseKeySem(key);
	}
	
	
	void DCputCausal(String key, String value, long adjustedtimestamp){
		ArrayList<StoreValue> tmplist;
		
		System.out.println("PUT:" + key + " Value:" + value + " timestamp:" + adjustedtimestamp);
		
		
		/* get ahead time */
		PriorityBlockingQueue<Long> tmpqueue = null;
		try {
			mutexPutTime.acquire();
		} catch (InterruptedException e) {}
		if (putTime.containsKey(key)){
			tmpqueue = putTime.get(key);
		}
		mutexPutTime.release();
		
		/* get store list */
		try {
			mutexStore.acquire();
		} catch (InterruptedException e) {}
		if (store.containsKey(key)){
			tmplist = store.get(key);
		} else{
			tmplist = new ArrayList<StoreValue>();
			store.put(key, tmplist);
		}
		mutexStore.release();
		
		/* wait for the right ahead time*/
		while((!tmpqueue.isEmpty()) && (tmpqueue.peek() < adjustedtimestamp)){
			//System.out.println("This is " + adjustedtimestamp + " head is " + tmpqueue.peek());
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {}
		}
		
		StoreValue sv = new StoreValue(adjustedtimestamp, value);
		
		acquireKeySem(key);
		tmplist.add(sv);
		tmpqueue.remove();
		releaseKeySem(key);
	}
	
	
	void DCputStrong(String key, String value, long adjustedtimestamp){
		ArrayList<StoreValue> tmplist;
		
		System.out.println("PUT:" + key + " Value:" + value + " timestamp:" + adjustedtimestamp);
		
		
		/* get ahead time */
		PriorityBlockingQueue<Long> tmpqueue = null;
		try {
			mutexPutTime.acquire();
		} catch (InterruptedException e) {}
		if (putTime.containsKey(key)){
			tmpqueue = putTime.get(key);
		}
		mutexPutTime.release();
		
		/* get store list */
		try {
			mutexStore.acquire();
		} catch (InterruptedException e) {}
		if (store.containsKey(key)){
			tmplist = store.get(key);
		} else{
			tmplist = new ArrayList<StoreValue>();
			store.put(key, tmplist);
		}
		mutexStore.release();
		
		/* wait for the right ahead time*/
		while(tmpqueue.peek() != adjustedtimestamp){
			//System.out.println("This is " + adjustedtimestamp + " head is " + tmpqueue.peek());
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {}
		}
		
		StoreValue sv = new StoreValue(adjustedtimestamp, value);
		
		acquireKeySem(key);
		tmplist.add(sv);
		releaseKeySem(key);
	}
	
	
	String DCgetStrong(String key, long timestamp){
		/* wait until all PUT operations with earlier timestamp are done*/
		PriorityBlockingQueue<Long> tmpqueue = null;
		try {
			mutexPutTime.acquire();
		} catch (InterruptedException e) {}
		if (putTime.containsKey(key)){
			tmpqueue = putTime.get(key);
		}
		mutexPutTime.release();
		
		while(tmpqueue != null && !tmpqueue.isEmpty()){
			if (timestamp <= tmpqueue.peek()){
				break;
			}
			try {
				//System.out.println("sleep for 100ms");
				Thread.sleep(100);
			} catch (InterruptedException e) {}
		}
		
		
		/* Only return values before the timestamp*/
		try {
			mutexStore.acquire();
		} catch (InterruptedException e) {}
		ArrayList<StoreValue> values = store.get(key);
		mutexStore.release();
		
		acquireKeySem(key);
		String response = "";
		if (values != null) {
			for (StoreValue val : values) {
				if (val.getTimestamp() < timestamp){
					response = response + val.getValue() + " ";
				}else{
					break;
				}
			}
		}
		releaseKeySem(key);
		System.out.println("returned response" + response);
		return response;
	}
	
	String DCgetCausal(String key, long timestamp){
				
		try {
			mutexStore.acquire();
		} catch (InterruptedException e) {}
		ArrayList<StoreValue> values = store.get(key);
		mutexStore.release();
		
		acquireKeySem(key);
		String response = "";
		if (values != null) {
			for (StoreValue val : values) {
					response = response + val.getValue() + " ";
			}
		}
		releaseKeySem(key);
		System.out.println("returned response" + response);
		return response;
	}
	
	String DCgetEventual(String key, long timestamp){
				
		try {
			mutexStore.acquire();
		} catch (InterruptedException e) {}
		ArrayList<StoreValue> values = store.get(key);
		mutexStore.release();
		
		acquireKeySem(key);
		String response = "";
		if (values != null) {
			for (StoreValue val : values) {
					response = response + val.getValue() + " ";
			}
		}
		releaseKeySem(key);
		System.out.println("returned response" + response);
		return response;
	}
	
	/* check and acquire the semaphore from map*/
	void acquireKeySem(String key){
		Semaphore tmpsem;
		try {
			mutexMapSemStore.acquire();
		} catch (InterruptedException e){}
		if (semStore.containsKey(key)){//if semaphore for this key has been initialized
    			tmpsem = semStore.get(key);
        }else {//if semaphore for this key has not been initialized
        	tmpsem = new Semaphore(1, false);
            semStore.put(key, tmpsem);
        }
		mutexMapSemStore.release();
		
		try{
			tmpsem.acquire();
		}catch (InterruptedException e){}
	}
	
	void releaseKeySem(String key){
		try {
			mutexMapSemStore.acquire();
		} catch (InterruptedException e){}
		Semaphore tmp = semStore.get(key);
		mutexMapSemStore.release();
		tmp.release();
	}
	/*
	void acquireQueueSem(String key){
		try {
			mutexMapSemQueue.acquire();
		} catch (InterruptedException e){}
		
		if (semQueue.containsKey(key)){//if semaphore for this key has been initialized
        	try{
        		semQueue.get(key).acquire();
    		}catch (InterruptedException e){}
        }else {//if semaphore for this key has not been initialized
        	semQueue.put(key, new Semaphore(1, false));
            try{
            	semQueue.get(key).acquire();
    		}catch (InterruptedException e){}
        }
		mutexMapSemQueue.release();
	}
	
	void releaseQueueSem(String key){
		try {
			mutexMapSemQueue.acquire();
		} catch (InterruptedException e){}
		Semaphore tmp = semQueue.get(key);
		mutexMapSemQueue.release();
		tmp.release();
	}
	*/
}
