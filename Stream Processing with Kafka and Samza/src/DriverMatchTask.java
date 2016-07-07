
package com.cloudcomputing.samza.pitt_cabs;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.util.HashMap;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider to
 * driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask, WindowableTask {

	/* Define per task state here. (kv stores etc) */
	private KeyValueStore<String, String> keyValue;

	@Override
	@SuppressWarnings("unchecked")
	public void init(Config config, TaskContext context) throws Exception {
		// Initialize stuff (maybe the kv stores?)
		keyValue = (KeyValueStore<String, String>) context.getStore("driver-loc");
	}

	@Override
	@SuppressWarnings("unchecked")
	public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
		// The main part of your code. Remember that all the messages for a
		// particular partition
		// come here (somewhat like MapReduce). So for task 1 messages for a
		// blockId will arrive
		// at one task only, thereby enabling you to do stateful stream
		// processing.
		String incomingStream = envelope.getSystemStreamPartition().getStream();
		if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {// driver
																						// loc
			Map<String, Object> map = (Map<String, Object>) envelope.getMessage();
			int blockId = (int) map.get("blockId");
			int driverId = (int) map.get("driverId");
			String type = (String) map.get("type");
			int latitude = (int) map.get("latitude");
			int longitude = (int) map.get("longitude");

			String key = Integer.toString(blockId) + " " + Integer.toString(driverId);
			String value = Integer.toString(latitude) + " " + Integer.toString(longitude);
			keyValue.put(key, value);
		} else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
			Map<String, Object> map = (Map<String, Object>) envelope.getMessage();
			int blockId = (int) map.get("blockId");
			String type = (String) map.get("type");
			int latitude = (int) map.get("latitude");
			int longitude = (int) map.get("longitude");
			if (type.equalsIgnoreCase("RIDE_REQUEST")) {// ride request
				int riderId = (int) map.get("riderId");
				KeyValueIterator<String, String> freeDrivers = keyValue.range(blockId + " ", blockId + " ");
				double minDis = Double.MAX_VALUE;
				int minId = 0;
				try {
					while (freeDrivers.hasNext()) {
						Entry<String, String> driver = freeDrivers.next();
						String key = driver.getKey();
						String value = driver.getValue();
						String[] words = value.split(" ");
						int tmplat = Integer.parseInt(words[0]);
						int tmplon = Integer.parseInt(words[1]);
						double dis = Math.pow(((double) (tmplat - latitude)), 2)
								+ Math.pow(((double) (tmplon - longitude)), 2);
						if (dis < minDis) {
							minDis = dis;
							minId = Integer.parseInt(key.split(" ")[1]);
						}
						HashMap<String, Object> message = new HashMap<String, Object>();
						message.put("riderId", riderId);
						message.put("driverId", minId);
						collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, message));
					}
				} finally {
					freeDrivers.close();
				}
			} else if (type.equalsIgnoreCase("RIDE_COMPLETE")) {// complete
				int driverId = (int) map.get("driverId");
				String key = Integer.toString(blockId) + " " + Integer.toString(driverId);
				String value = Integer.toString(latitude) + " " + Integer.toString(longitude);
				keyValue.put(key, value);
			} else if (type.equalsIgnoreCase("LEAVING_BLOCK")) {// leaving
				int driverId = (int) map.get("driverId");
				String status = (String) map.get("status");
				String key = Integer.toString(blockId) + " " + Integer.toString(driverId);
				keyValue.delete(key);

			} else if (type.equalsIgnoreCase("ENTERING_BLOCK")) {// entering
				int driverId = (int) map.get("driverId");
				String status = (String) map.get("status");
				String key = Integer.toString(blockId) + " " + Integer.toString(driverId);
				if (status.equalsIgnoreCase("AVAILABLE")) {
					String value = Integer.toString(latitude) + " " + Integer.toString(longitude);
					keyValue.put(key, value);
				} else {
					keyValue.delete(key);
				}
			}
		} else {
			throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
		}

	}

	@Override
	public void window(MessageCollector collector, TaskCoordinator coordinator) {
		// this function is called at regular intervals, not required for this
		// project
	}
}
