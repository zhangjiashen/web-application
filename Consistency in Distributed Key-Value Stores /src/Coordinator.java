import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.LinkedList;
import java.util.Map;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;
/* Strong */
public class Coordinator extends Verticle {

	// This integer variable tells you what region you are in
	// 1 for US-E, 2 for US-W, 3 for Singapore
	private static int region = KeyValueLib.region;

	// Default mode: Strongly consistent
	// Options: causal, eventual, strong
	private static String consistencyType = "strong";

	/**
	 * TODO: Set the values of the following variables to the DNS names of your
	 * three dataCenter instances. Be sure to match the regions with their DNS!
	 * Do the same for the 3 Coordinators as well.
	 */
	private static final String dataCenterUSE = "ec2-52-91-55-220.compute-1.amazonaws.com";
	private static final String dataCenterUSW = "ec2-54-208-103-111.compute-1.amazonaws.com";
	private static final String dataCenterSING = "ec2-52-23-162-4.compute-1.amazonaws.com";

	private static final String coordinatorUSE = "ec2-54-208-184-32.compute-1.amazonaws.com";
	private static final String coordinatorUSW = "ec2-52-91-140-222.compute-1.amazonaws.com";
	private static final String coordinatorSING = "ec2-54-85-140-148.compute-1.amazonaws.com";

	//mutex for each key
	public static HashMap<String, Semaphore> sem = new HashMap<String, Semaphore> ();
	
	
	private class KeyVal implements Comparator<KeyVal>, Comparable<KeyVal>{
		public String key;
		public String val;// for put, this is value. for get, this is loc.
		public long timeStamp;
		boolean putget;//true for put, false for get
		boolean forwarded;
		
		public int compareTo(KeyVal kv){
			return Long.compare(this.timeStamp, kv.timeStamp);
		}
		
		public int compare(KeyVal x, KeyVal y){
			return Long.compare(x.timeStamp, y.timeStamp);
		}
		
		public KeyVal(String key, String val, long timeStamp, boolean putget, boolean forwarded){
			this.key = key;
			this.val = val;
			this.timeStamp = timeStamp;
			this.putget = putget;
			this.forwarded = forwarded;
		}
		public KeyVal(){}
	}
	public Coordinator(){
		Coordinator.queue = new HashMap<String, PriorityBlockingQueue<KeyVal>> (); //new PriorityBlockingQueue<KeyVal>(20, new KeyVal()); 
	}
	
	public static HashMap<String, PriorityBlockingQueue<KeyVal>> queue;
	private static Semaphore mutexQueue = new Semaphore(1,false);
	private static Semaphore mutexMapSem = new Semaphore(1,false);
	@Override
	public void start() {
		KeyValueLib.dataCenters.put(dataCenterUSE, 1);
		KeyValueLib.dataCenters.put(dataCenterUSW, 2);
		KeyValueLib.dataCenters.put(dataCenterSING, 3);
		KeyValueLib.coordinators.put(coordinatorUSE, 1);
		KeyValueLib.coordinators.put(coordinatorUSW, 2);
		KeyValueLib.coordinators.put(coordinatorSING, 3);
		
		
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
				final String timestring = map.get("timestamp");
				final Long timestamp = Long.parseLong(timestring);
				final String forwarded = map.get("forward");
				final String forwardedRegion = map.get("region");
				
				
				
				final boolean isforwarded;
				if (forwarded != null){
					isforwarded = forwarded.equalsIgnoreCase("true");
				} else {
					isforwarded = false;
				}
				
				final long adjustedtimestamp;
				if ((consistencyType.equals("strong") || consistencyType.equals("causal")) && isforwarded){
					adjustedtimestamp = Skews.handleSkew(timestamp, Integer.parseInt(forwardedRegion));
				} else {
					adjustedtimestamp = timestamp;
				}
				
				System.out.println("PUT" + " key:" + key + " value:" + value + " timestamp" + adjustedtimestamp + isforwarded + " from" + forwardedRegion + " " + System.currentTimeMillis());
				
				/*enqueue*/
				
				if (consistencyType.equals("strong") || consistencyType.equals("causal")){
					enqueue(key, value, adjustedtimestamp, true, isforwarded);
				}
				//System.out.println(queue.toString());
				Thread t = new Thread(new Runnable() {
					public void run() {
						switch (consistencyType){
						case "strong":
							putStrong(key, value, adjustedtimestamp, isforwarded);
							break;
						case "causal":
							putCausal(key, value, adjustedtimestamp, isforwarded);
							break;
						case "eventual":
							putEventual(key, value, adjustedtimestamp, isforwarded);
							break;
						default:
							System.out.println("unknown consistency type: " + consistencyType);
						}
					}
				});
				t.start();
				req.response().end(); // Do not remove this
			}
		});

		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final Long timestamp = Long.parseLong(map.get("timestamp"));
				
				/*enqueue*/
				
				System.out.println("GET " + "key:" + key + " timestamp" + timestamp);
				if (consistencyType.equals("strong") || consistencyType.equals("causal")){
					enqueue(key, null, timestamp, false, false);
				}
				Thread t = new Thread(new Runnable() {
					public void run() {

						String response = "0";
						switch (consistencyType){
						case "strong":
							response = getStrong(key, timestamp);
							break;
						case "causal":
							response = getCausal(key, timestamp);
							break;
						case "eventual":
							response = getEventual(key, timestamp);
							break;
						default:
							System.out.println("unknown consistency type: " + consistencyType);
						}
						req.response().end(response);
					}
				});
				t.start();
			}
		});
		/* This endpoint is used by the grader to change the consistency level */
		routeMatcher.get("/consistency", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				consistencyType = map.get("consistency");
				req.response().end();
			}
		});
		/* BONUS HANDLERS BELOW */
		routeMatcher.get("/forwardcount", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				req.response().end(KeyValueLib.COUNT());
			}
		});

		routeMatcher.get("/reset", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				System.out.println("*************************Reset***************************");
				KeyValueLib.RESET();
				mutexMapSem = null;
				mutexMapSem = new Semaphore(1,false);
				sem.clear();
				queue.clear();
				mutexQueue = null;
				mutexQueue = new Semaphore(1,false);
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
	
	/* put thread routine*/
	void putEventual(final String key, final String value, long adjustedtimestamp, boolean isforwarded){
		PriorityBlockingQueue<KeyVal> tmpqueue = getqueue(key);
		final String timestring = Long.toString(adjustedtimestamp);
	
		/* AHEAD and FORWARD*/
		if (!isforwarded){//call AHEAD only once (at the first time a PUT arrives at any coordinator)
			int hashcode = myHash(key) + 1;// 1, 2, or 3
			//System.out.println("hashcode: " + hashcode);
			// forward to the right Coodinator
			if (hashcode != region){
				System.out.println("Forward PUT " + key);
				try{
					switch (hashcode) {
					case Constants.US_EAST:
						KeyValueLib.FORWARD(coordinatorUSE, key, value, timestring);
						break;
					case Constants.US_WEST:
						KeyValueLib.FORWARD(coordinatorUSW, key, value, timestring);
						break;
					case Constants.SINGAPORE:
						KeyValueLib.FORWARD(coordinatorSING, key, value, timestring);
						break;
					}
				} catch (IOException e){
					e.printStackTrace();
				}
				//releaseKeySem(key);
				return;
			}
		}
		
		/* Operations for the primary coordinator of the key*/
		
		
		Thread t1 = new Thread(new Runnable() {
			public void run() {
				try {
					KeyValueLib.PUT(dataCenterUSE, key, value, timestring, consistencyType);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		t1.start();
		
		Thread t2 = new Thread(new Runnable() {
			public void run() {
				try {
					KeyValueLib.PUT(dataCenterUSW, key, value, timestring, consistencyType);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		t2.start();
		
		Thread t3 = new Thread(new Runnable() {
			public void run() {
				try {
					KeyValueLib.PUT(dataCenterSING, key, value, timestring, consistencyType);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		t3.start();
	
	}
	
	
	/* put thread routine*/
	void putCausal(final String key, final String value, long adjustedtimestamp, boolean isforwarded){
		PriorityBlockingQueue<KeyVal> tmpqueue = getqueue(key);
		final String timestring = Long.toString(adjustedtimestamp);
		/* pop from queue*/
		while (true){
			acquireKeySem(key);
			KeyVal queuehead = tmpqueue.peek();
			
			//if the head is the right element
			if ((queuehead != null) && queuehead.putget && (queuehead.key.equals(key)) && (queuehead.val.equals(value)) && (queuehead.timeStamp == adjustedtimestamp)){
				try {
					tmpqueue.take();
				} catch (InterruptedException e) {}
				releaseKeySem(key);
				System.out.println("dequeue PUT key: " + key + " timestamp: " + timestring);
				break;
			} else {// if the head is not the right element
				releaseKeySem(key);
				System.out.println("Not right head, wait for 50ms.");
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {}
			}
		}
		
		/* AHEAD and FORWARD*/
		if (!isforwarded){//call AHEAD only once (at the first time a PUT arrives at any coordinator)
			try {
				KeyValueLib.AHEAD(key, timestring);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			int hashcode = myHash(key) + 1;// 1, 2, or 3
			//System.out.println("hashcode: " + hashcode);
			// forward to the right Coodinator
			if (hashcode != region){
				System.out.println("Forward PUT " + key);
				try{
					switch (hashcode) {
					case Constants.US_EAST:
						KeyValueLib.FORWARD(coordinatorUSE, key, value, timestring);
						break;
					case Constants.US_WEST:
						KeyValueLib.FORWARD(coordinatorUSW, key, value, timestring);
						break;
					case Constants.SINGAPORE:
						KeyValueLib.FORWARD(coordinatorSING, key, value, timestring);
						break;
					}
				} catch (IOException e){
					e.printStackTrace();
				}
				//releaseKeySem(key);
				return;
			}
		}
		
		/* Operations for the primary coordinator of the key*/
		
		
		Thread t1 = new Thread(new Runnable() {
			public void run() {
				try {
					KeyValueLib.PUT(dataCenterUSE, key, value, timestring, consistencyType);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		t1.start();
		
		Thread t2 = new Thread(new Runnable() {
			public void run() {
				try {
					KeyValueLib.PUT(dataCenterUSW, key, value, timestring, consistencyType);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		t2.start();
		
		Thread t3 = new Thread(new Runnable() {
			public void run() {
				try {
					KeyValueLib.PUT(dataCenterSING, key, value, timestring, consistencyType);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		t3.start();
	
	}
	
	
	/* put thread routine*/
	void putStrong(final String key, final String value, long adjustedtimestamp, boolean isforwarded){
		PriorityBlockingQueue<KeyVal> tmpqueue = getqueue(key);
		final String timestring = Long.toString(adjustedtimestamp);
		/* pop from queue*/
		while (true){
			acquireKeySem(key);
			KeyVal queuehead = tmpqueue.peek();
			
			//if the head is the right element
			if ((queuehead != null) && queuehead.putget && (queuehead.key.equals(key)) && (queuehead.val.equals(value)) && (queuehead.timeStamp == adjustedtimestamp)){
				try {
					tmpqueue.take();
				} catch (InterruptedException e) {}
				releaseKeySem(key);
				System.out.println("dequeue PUT key: " + key + " timestamp: " + timestring);
				break;
			} else {// if the head is not the right element
				releaseKeySem(key);
				System.out.println("Not right head, wait for 50ms.");
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {}
			}
		}
		
		/* AHEAD and FORWARD*/
		if (!isforwarded){//call AHEAD only once (at the first time a PUT arrives at any coordinator)
			try {
				KeyValueLib.AHEAD(key, timestring);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			int hashcode = myHash(key) + 1;// 1, 2, or 3
			//System.out.println("hashcode: " + hashcode);
			// forward to the right Coodinator
			if (hashcode != region){
				System.out.println("Forward PUT " + key);
				try{
					switch (hashcode) {
					case Constants.US_EAST:
						KeyValueLib.FORWARD(coordinatorUSE, key, value, timestring);
						break;
					case Constants.US_WEST:
						KeyValueLib.FORWARD(coordinatorUSW, key, value, timestring);
						break;
					case Constants.SINGAPORE:
						KeyValueLib.FORWARD(coordinatorSING, key, value, timestring);
						break;
					}
				} catch (IOException e){
					e.printStackTrace();
				}
				//releaseKeySem(key);
				return;
			}
		}
		
		/* Operations for the primary coordinator of the key*/
		
		
		Thread t1 = new Thread(new Runnable() {
			public void run() {
				try {
					KeyValueLib.PUT(dataCenterUSE, key, value, timestring, consistencyType);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		t1.start();
		
		Thread t2 = new Thread(new Runnable() {
			public void run() {
				try {
					KeyValueLib.PUT(dataCenterUSW, key, value, timestring, consistencyType);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		t2.start();
		
		Thread t3 = new Thread(new Runnable() {
			public void run() {
				try {
					KeyValueLib.PUT(dataCenterSING, key, value, timestring, consistencyType);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		t3.start();
	
		/* wait for the threads finishing */
		
		try {
			t1.join();
		} catch (InterruptedException e) {}
		try {
			t2.join();
		} catch (InterruptedException e) {}
		try {
			t3.join();
		} catch (InterruptedException e) {}
		
		/* call COMPLETE */
		try {
			KeyValueLib.COMPLETE(key, timestring);
			System.out.println("Finished PUT" + "key:" + key + "value:" + value + "timestamp" + adjustedtimestamp + isforwarded);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	
	String getStrong(String key, long timestamp){
		String timestring = Long.toString(timestamp);
		PriorityBlockingQueue<KeyVal> tmpqueue = getqueue(key);
		/* pop from queue*/
		while (true){
			System.out.println("try to acquire sem in getStrong");
			acquireKeySem(key);
			KeyVal queuehead = tmpqueue.peek();
			//if the head is the according element
			if ((queuehead != null) && (!queuehead.putget) && (queuehead.key.equals(key)) && (queuehead.timeStamp == timestamp)){
				try {
					tmpqueue.remove();
				} catch (Exception e) {}
				releaseKeySem(key);
				break;
			} else {// if the head is not the according element
				releaseKeySem(key);
				System.out.println("Not right head, wait for 5ms.");
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {}
			}
		}
		

		String response = null;
		try{
			System.out.println("call /get of DC, key" + key);
			switch (region) {
			case Constants.US_EAST:
				response = KeyValueLib.GET(dataCenterUSE, key, timestring, consistencyType);
				break;
			case Constants.US_WEST:
				response = KeyValueLib.GET(dataCenterUSW, key, timestring, consistencyType);
				break;
			case Constants.SINGAPORE:
				response = KeyValueLib.GET(dataCenterSING, key, timestring, consistencyType);
				break;
			}
		} catch (IOException e){
			e.printStackTrace();
		}
		
		return response;
	}
	
	
	String getCausal(String key, long timestamp){
		String timestring = Long.toString(timestamp);
		PriorityBlockingQueue<KeyVal> tmpqueue = getqueue(key);
		/* pop from queue*/
		while (true){
			System.out.println("try to acquire sem in getStrong");
			acquireKeySem(key);
			KeyVal queuehead = tmpqueue.peek();
			//if the head is the according element
			if ((queuehead != null) && (!queuehead.putget) && (queuehead.key.equals(key)) && (queuehead.timeStamp == timestamp)){
				try {
					tmpqueue.remove();
				} catch (Exception e) {}
				releaseKeySem(key);
				break;
			} else {// if the head is not the according element
				releaseKeySem(key);
				System.out.println("Not right head, wait for 5ms.");
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {}
			}
		}
		

		String response = null;
		try{
			System.out.println("call /get of DC, key" + key);
			switch (region) {
			case Constants.US_EAST:
				response = KeyValueLib.GET(dataCenterUSE, key, timestring, consistencyType);
				break;
			case Constants.US_WEST:
				response = KeyValueLib.GET(dataCenterUSW, key, timestring, consistencyType);
				break;
			case Constants.SINGAPORE:
				response = KeyValueLib.GET(dataCenterSING, key, timestring, consistencyType);
				break;
			}
		} catch (IOException e){
			e.printStackTrace();
		}
		
		return response;
	}
	
	
	String getEventual(String key, long timestamp){
		String timestring = Long.toString(timestamp);
		String response = null;
		try{
			System.out.println("call /get of DC, key" + key);
			switch (region) {
			case Constants.US_EAST:
				response = KeyValueLib.GET(dataCenterUSE, key, timestring, consistencyType);
				break;
			case Constants.US_WEST:
				response = KeyValueLib.GET(dataCenterUSW, key, timestring, consistencyType);
				break;
			case Constants.SINGAPORE:
				response = KeyValueLib.GET(dataCenterSING, key, timestring, consistencyType);
				break;
			}
		} catch (IOException e){
			e.printStackTrace();
		}
		
		return response;
	}
	
	/* check and acquire the semaphore from map*/
	void acquireKeySem(String key){
		Semaphore tmpsem;
		try {
			mutexMapSem.acquire();
		} catch (InterruptedException e){}
		if (sem.containsKey(key)){//if semaphore for this key has been initialized
    			tmpsem = sem.get(key);
        }else {//if semaphore for this key has not been initialized
        	tmpsem = new Semaphore(1, false);
            sem.put(key, tmpsem);
        }
		mutexMapSem.release();
		
		try{
			tmpsem.acquire();
		}catch (InterruptedException e){}
	}
	
	void releaseKeySem(String key){
		try {
			mutexMapSem.acquire();
		} catch (InterruptedException e){}
		Semaphore tmp = sem.get(key);
		mutexMapSem.release();
		tmp.release();
	}
	
	void enqueue (String key, String value, long timestamp, boolean isput, boolean isforwarded){
		PriorityBlockingQueue<KeyVal> tmpqueue;
		try {
			mutexQueue.acquire();
		} catch (InterruptedException e){}
		if (queue.containsKey(key)){//if queue for this key has been initialized
			tmpqueue = queue.get(key);
		}else {//if queue for this key has not been initialized
			tmpqueue = new PriorityBlockingQueue<KeyVal>(20, new KeyVal());
			queue.put(key, tmpqueue);
		}
		mutexQueue.release();
	
		tmpqueue.add(new KeyVal(key, value, timestamp, isput, isforwarded));
		System.out.println("Added to queue. Queue head:" + tmpqueue.element().timeStamp + " Queue size:" + tmpqueue.size());
	}
	
	PriorityBlockingQueue<KeyVal> getqueue(String key){
		PriorityBlockingQueue<KeyVal> tmpqueue;
		try {
			mutexQueue.acquire();
		} catch (InterruptedException e){}
		tmpqueue = queue.get(key);
		mutexQueue.release();
		return tmpqueue;
	}
	
	/* compute hash */
	int myHash(String key){
		int i;
		int sum = 1 + key.length();
		for (i=0; i<key.length(); i++){
			sum += key.charAt(i);
		}
		//System.out.println(key + sum%3);
		return (sum%3);
	}
}
