import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;

public class MysqlServer extends AbstractVerticle {
	private static final String teamId = "Yamete";
	private static final String teamAWSAccountId = "148723206667";
	private static final int clientCount = 5;
	private static final int frontendCount = 3;
	// private static final String mySqlUrl =
	// "jdbc:mysql://52.23.176.79:3306/tweets_text";
	private static final String mySqlDriverClass = "com.mysql.jdbc.Driver";
	private static final String username = "root";
	private static final String password = "admin";
	private static ArrayList<JDBCClient> clients = new ArrayList<JDBCClient>(clientCount);
	private static HttpClient forwardClient;
	//private static ArrayList<HttpClient> forwardClients = new ArrayList<HttpClient>(frontendCount);
	private static ArrayList<String> ips = new ArrayList<String>(clientCount);
	private static ArrayList<String> frontendIps = new ArrayList<String>(frontendCount);
	private static final String key_X_string = "8271997208960872478735181815578166723519929177896558845922250595511921395049126920528021164569045773";

	// used to protect HashMaps from recreating a same entry
	private static Semaphore listMapSem = new Semaphore(1, false);
	// used as list entry, <tid, list>
	private static HashMap<String, ArrayList<Q6Data>> listMap = new HashMap<String, ArrayList<Q6Data>>();
	// used to store tags, <tweetid, tag>
	private static ConcurrentHashMap<String, String> tags = new ConcurrentHashMap<String, String>();
	// used for keeping 5 requests within the same tid in order, <tid,
	// semaphore>
	private static ConcurrentHashMap<String, Semaphore> finishSemMap = new ConcurrentHashMap<String, Semaphore>();

	private final static int currentServerId = 2;

	private void configDataCenters() {
        ips.add("54.175.24.114");
        ips.add("54.208.158.73");
        ips.add("54.175.162.177");
        ips.add("54.152.77.24");
        ips.add("54.208.166.252");
	}

	private void configFrontend() {
		frontendIps.add("54.164.114.156");
		frontendIps.add("54.172.183.213");
		frontendIps.add("54.164.191.111");
	}
	private static int keyZ(String key_XY_string) {
		int key_Z;
		int a = Integer.parseInt(key_X_string.substring(key_X_string.length() - 2)) % 25;
		int c = Integer.parseInt(key_XY_string.substring(key_XY_string.length() - 2)) % 25;
		int b;
		for (b = 0; b < 25; b++) {
			if ((a * b - c) % 25 == 0)
				return b + 1;
		}
		return -1;
	}

	private static String decode(String secret_message, int key_Z) {
		char[] intermediate_message = new char[secret_message.length()];
		int arrlen = (int) Math.sqrt((double) secret_message.length());
		int i, j;
		int count = 0;
		for (i = 0; i < arrlen; i++) {
			for (j = 0; j <= i; j++) {
				intermediate_message[count] = Character
						.toChars('A' + (secret_message.charAt(j * arrlen + i - j) + 26 - key_Z - 'A') % 26)[0];
				count++;
			}
		}
		int k = 1;
		for (i = arrlen; i < 2 * arrlen - 1; i++) {
			for (j = k; j < arrlen; j++) {
				intermediate_message[count] = Character
						.toChars('A' + (secret_message.charAt(j * arrlen + i - j) + 26 - key_Z - 'A') % 26)[0];
				count++;
			}
			k++;
		}

		String ret = new String(intermediate_message);
		return ret;
	}

	private int getDataCenter(String s) {
		byte[] sByte = s.getBytes();
		int sum = 0;
		for (byte b : sByte) {
			sum += b;
		}
		int ret = sum % clientCount;
		return ret >= 0 ? ret : (ret + clientCount) % clientCount;
	}

	public void start() {
		System.out.println("currentServerId" + currentServerId);
		HttpServer server = vertx.createHttpServer();
		Router router = Router.router(vertx);
		configDataCenters();
		configFrontend();
		Q3DataComparatorPos cmpPos = new Q3DataComparatorPos();
		Q3DataComparatorNeg cmpNeg = new Q3DataComparatorNeg();
		for (int i = 0; i < clientCount; ++i) {
			final JDBCClient client = JDBCClient.createShared(vertx,
					new JsonObject().put("url", "jdbc:mysql://" + ips.get(i) + ":3306/tweets_text")
							.put("driver_class", mySqlDriverClass).put("user", username).put("password", password)
							.put("max_pool_size", 4000),
					"MySQL Datasource" + i);
			clients.add(client);
		}
		forwardClient =  vertx.createHttpClient();
		Date date = new Date();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String time = df.format(date);
		router.route("/q0").handler(routingContext -> {
			HttpServerResponse response = routingContext.response();
			response.putHeader("content-type", "text/html");
			response.end("200");
		});
		router.route("/q1").handler(routingContext -> {
			String key = routingContext.request().getParam("key");
			String msg = routingContext.request().getParam("message");
			String message = decode(msg, keyZ(key));
			String output = new String();
			output = teamId + "," + teamAWSAccountId + '\n' + time + '\n' + message + '\n';
			// This handler will be called for every request
			HttpServerResponse response = routingContext.response();
			response.putHeader("content-type", "text/html");
			// Write to the response and end it
			response.end(output);
		});
		router.route("/q2").handler(routingContext -> {
			// This handler will be called for every request
			HttpServerResponse response = routingContext.response();
			response.putHeader("content-type", "text/html;charset=utf8");
			// response.putHeader("Content-Length", "200");
			final String userId = routingContext.request().getParam("userid");
			final String tweetTime = routingContext.request().getParam("tweet_time");

			Thread t = new Thread(new Runnable() {
				public void run() {
					String output = teamId + "," + teamAWSAccountId + '\n';
					int clientId = getDataCenter(userId + tweetTime);
					String sql = "SELECT tweet_id,score,tweet_text FROM tweets WHERE userid=\"" + userId
							+ "\" AND tweet_time=\"" + tweetTime + "\" order by tweet_id";
					// Vertx has built-in connection pool

					clients.get(clientId).getConnection(res -> {
						if (res.succeeded()) {
							SQLConnection connection = res.result();
							connection.query(sql, res2 -> {
								if (res2.succeeded()) {
									ResultSet rs = res2.result();
									List<JsonArray> results = rs.getResults();
									// Do something with results
									String data = "";
									for (JsonArray row : results) {
										String tweet_id = row.getString(0);
										String score = row.getString(1);
										String tweet_text = "";
										try {
											tweet_text = java.net.URLDecoder.decode(row.getString(2), "UTF8");
										} catch (Exception e) {
											// TODO Auto-generated catch block
											e.printStackTrace();
										}
										data += (tweet_id + ":" + score + ":" + tweet_text + "\n");
									}
									response.end(output + data);
								} else {
									System.out.println("Fail to excute sql query.");
									response.end("Fail to excute sql query.");
								}

							});
							connection.close();
						} else {
							// Failed to get connection - deal with it
							System.out.println("Fail to connect mysql.");
							response.end("Fail to connect mysql.");
						}
					});
				}
			});
			t.start();

		});
		router.route("/q3").handler(routingContext -> {
			// This handler will be called for every request
			HttpServerResponse response = routingContext.response();
			response.putHeader("content-type", "text/html;charset=utf8");
			// response.putHeader("Content-Length", "200");
			final String startDate = routingContext.request().getParam("start_date");
			final String endDate = routingContext.request().getParam("end_date");
			final String userId = routingContext.request().getParam("userid");
			final String number = routingContext.request().getParam("n");
			String output = teamId + "," + teamAWSAccountId + '\n';
			int clientId = getDataCenter(userId);

			String sql = "select tweet_time, impact_score, tweet_id, tweet_text from q3 where " + "tweet_time<=\""
					+ endDate + "\" and tweet_time>=\"" + startDate + "\" and user_id=\"" + userId + "\"";
			Thread t = new Thread(new Runnable() {
				public void run() {
					clients.get(clientId).getConnection(res -> {
						if (res.succeeded()) {
							SQLConnection connection = res.result();
							connection.query(sql, res2 -> {
								if (res2.succeeded()) {
									ResultSet rs = res2.result();
									List<JsonArray> results = rs.getResults();
									ArrayList<Q3Data> dataListPos = new ArrayList<Q3Data>();
									ArrayList<Q3Data> dataListNeg = new ArrayList<Q3Data>();
									for (JsonArray row : results) {
										String tweetTime = row.getString(0);

										int impactScore = row.getInteger(1);
										String tweetId = row.getString(2);
										String tweetText = "";
										try {
											tweetText = java.net.URLDecoder.decode(row.getString(3), "UTF8");
										} catch (Exception e) {
											// TODO Auto-generated catch block
											e.printStackTrace();
										}
										if (impactScore > 0) {
											dataListPos.add(new Q3Data(tweetTime, impactScore, tweetId, tweetText));
										} else if (impactScore < 0) {
											dataListNeg.add(new Q3Data(tweetTime, impactScore, tweetId, tweetText));
										}
									}
									Collections.sort(dataListPos, cmpPos);
									Collections.sort(dataListNeg, cmpNeg);
									int len = dataListPos.size();
									int n = Integer.parseInt(number);
									String dataStr = "Positive Tweets\n";
									for (int i = 0; i < n; ++i) {
										if (i >= len)
											break;
										dataStr += (dataListPos.get(i).getTweetTime() + ","
												+ dataListPos.get(i).getImpactScore() + ","
												+ dataListPos.get(i).getTweetId() + ","
												+ dataListPos.get(i).getTweetText() + "\n");
									}
									dataStr += "\nNegative Tweets\n";
									len = dataListNeg.size();
									for (int i = 0; i < n; ++i) {
										if (i >= len)
											break;
										dataStr += (dataListNeg.get(i).getTweetTime() + ","
												+ dataListNeg.get(i).getImpactScore() + ","
												+ dataListNeg.get(i).getTweetId() + ","
												+ dataListNeg.get(i).getTweetText() + "\n");
									}
									response.end(output + dataStr);

								} else {
									System.out.println("Fail to excute sql query.");
									response.end("Fail to excute sql query.");
								}
							});
							connection.close();
						} else {
							// Failed to get connection - deal with it
							System.out.println("Fail to connect mysql.");
							response.end("Fail to connect mysql.");
						}
					});
				}
			});
			t.start();
		});
		router.route("/q4").handler(routingContext -> {
			// This handler will be called for every request
			HttpServerResponse response = routingContext.response();
			response.putHeader("content-type", "text/html;charset=utf8");
			// response.putHeader("Content-Length", "200");
			final String hashTag = routingContext.request().getParam("hashtag");
			final String number = routingContext.request().getParam("n");
			String output = teamId + "," + teamAWSAccountId + '\n';
			String encodedHashTag = "";
			try {
				encodedHashTag = java.net.URLEncoder.encode(hashTag, "UTF8");
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			// System.out.println(encodedHashTag);
			int clientId = getDataCenter(encodedHashTag);
			// System.out.println(ips.get(clientId));
			String sql = "select tweet_time, count, user_list, tweet_text from q4 where hashtag= binary \""
					+ encodedHashTag + "\" order by count desc, tweet_time asc limit " + number;
			Thread t = new Thread(new Runnable() {
				public void run() {
					clients.get(clientId).getConnection(res -> {
						if (res.succeeded()) {
							SQLConnection connection = res.result();
							connection.query(sql, res2 -> {
								if (res2.succeeded()) {
									ResultSet rs = res2.result();
									List<JsonArray> results = rs.getResults();
									String data = "";
									for (JsonArray row : results) {
										String tweetTime = row.getString(0);
										int count = row.getInteger(1);
										String userList = row.getString(2);
										String tweetText = "";
										try {
											tweetText = java.net.URLDecoder.decode(row.getString(3), "UTF8");
										} catch (Exception e) {
											// TODO Auto-generated catch block
											e.printStackTrace();
										}
										data += (tweetTime + ":" + count + ":" + userList + ":" + tweetText + "\n");
									}
									response.end(output + data);
								} else {
									System.out.println("Fail to excute sql query.");
									response.end("Fail to excute sql query.");
								}
							});
							connection.close();
						} else {
							System.out.println("Fail to connect mysql.");
							response.end("Fail to connect mysql.");
						}
					});
				}
			});
			t.start();
		});
		router.route("/q6").handler(routingContext -> {
			// This handler will be called for every request
			HttpServerResponse response = routingContext.response();
			response.putHeader("content-type", "text/html;charset=utf8");
			// response.putHeader("Content-Length", "200");
			final String tid = routingContext.request().getParam("tid");
			final String path = routingContext.request().path();
			final String query = routingContext.request().query();
			int tidint = Integer.parseInt(tid);
			int serverId = transHash(tidint);
			
			if (serverId != currentServerId) {
				// forward to server and wait for response
				//System.out.println("forwarded to " + serverId);
				
				Thread t2 = new Thread(new Runnable() {
					public void run() {
						String host = frontendIps.get(serverId);
						HttpClient client = vertx.createHttpClient();
						client.getNow(80, host, path + "?" + query, new Handler<HttpClientResponse>() {
							@Override
							public void handle(HttpClientResponse httpClientResponse) {
								httpClientResponse.bodyHandler(new Handler<Buffer>() {
									@Override
									public void handle(Buffer buffer) {
										response.end(buffer.getString(0, buffer.length()));
										client.close();
									}
								});
							}
						});
					}
				});
				t2.start();
			} else {// correct server

				final String opt = routingContext.request().getParam("opt");
				String output = teamId + "," + teamAWSAccountId + '\n';
				if (opt.equals("s") || opt.equals("e")) {// start or end
					response.end(output + "0\n");
				} else {
					final String seq;
					final String tweetid;
					final boolean optBool;

					final String tag;
					if (opt.equals("a")) {// append
						optBool = true;
						tag = routingContext.request().getParam("tag");
						seq = routingContext.request().getParam("seq");
						tweetid = routingContext.request().getParam("tweetid");
					} else {// read
						optBool = false;
						tag = null;
						seq = routingContext.request().getParam("seq");
						tweetid = routingContext.request().getParam("tweetid");
					}
					
					Thread t = new Thread(new Runnable() {
						public void run() {
							ArrayList<Q6Data> list;

							if (optBool) {// if opt is append, response
											// immediately
								response.end(output + tag + "\n");
							}

							// access list map
							try {
								listMapSem.acquire();
							} catch (InterruptedException e1) {
							}

							if (listMap.containsKey(tid)) {// not the first
								list = listMap.get(tid);
								listMapSem.release();
							} else {// first occur
								list = new ArrayList<Q6Data>(7);
								listMap.put(tid, list);
								finishSemMap.putIfAbsent(tid, new Semaphore(1, false));
								listMapSem.release();
							}

							Semaphore sem = new Semaphore(0, false);
							list.add(new Q6Data(Integer.parseInt(seq), tweetid, sem));
							//System.out.println("Added to list: " + list.size());
							if (list.size() == 5) {// all 5 requests arrived
								// start a new thread to wake the waiting 5
								// requests in order
								
								Thread t1 = new Thread(new Runnable() {
									public void run() {
										Collections.sort(list, new Q6DataComparator());
										//System.out.println("start a new thread to wake the waiting 5");

										ArrayList<String> tweetidToClean = new ArrayList<String>(2);
										for (Q6Data q6data : list) {
											// store tweetid for cleaning up
											tweetidToClean.add(q6data.getTweetid());
											try {
												finishSemMap.get(tid).acquire();
											} catch (InterruptedException e) {
											}
											// invoke the next request
											//System.out.println("invoke the next request");
											q6data.getSem().release();

										}

										// clean this tid after the last request
										// finished
										try {
											finishSemMap.get(tid).acquire();
										} catch (InterruptedException e) {
										}
										//System.out.println("Begin clean up!");
										finishSemMap.remove(tid);
										listMap.remove(tid);
										for (String tweet : tweetidToClean) {
											tags.remove(tweet);
										}
									}
								});
								t1.start();
							}

							// wait for its turn
							try {
								sem.acquire();
							} catch (InterruptedException e) {
							}

							//System.out.println("seq " + seq + " turn!");
							// data center operations here
							if (optBool) {// append
								tags.put(tweetid, tag);
								//System.out.println("Put tag: " + tags.get(tweetid));
								// execute next request in this tid
								finishSemMap.get(tid).release();
							} else {// read
								int clientId = getDataCenter(tweetid);
								String sql = "select tweet_text from q6 where tweet_id=\"" + tweetid +"\"";
								clients.get(clientId).getConnection(res -> {
									//System.out.println("clientId: " + clientId);
									if (res.succeeded()) {
										SQLConnection connection = res.result();
										connection.query(sql, res2 -> {
											if (res2.succeeded()) {
												ResultSet rs = res2.result();
												List<JsonArray> results = rs.getResults();
												String data = "";
												for (JsonArray row : results) {
													String tweetText = "";
													try {
														tweetText = java.net.URLDecoder.decode(row.getString(0),
																"UTF8");
													} catch (Exception e) {
														e.printStackTrace();
													}
													data += (tweetText);
												}
												// execute next request in this tid
												
												if (tags.containsKey(tweetid)){
													response.end(output + data + tags.get(tweetid) + "\n");
													finishSemMap.get(tid).release();
												} else {
													response.end(output + data + "\n");
													finishSemMap.get(tid).release();
												}
											} else {
												System.out.println("Fail to excute sql query.");
												// execute next request in this tid
												finishSemMap.get(tid).release();
												response.end("Fail to excute sql query.");
											}
										});
										connection.close();
									} else {
										System.out.println("Fail to connect mysql.");
										// execute next request in this tid
										finishSemMap.get(tid).release();
										response.end("Fail to connect mysql.");
									}
									
								});
							}

							

						}
					});
					t.start();
				}
			}

		});
		server.requestHandler(router::accept).listen(80);

	}

	private int transHash(int tid) {
		int hash = (tid & 0x11) % 3;
		return hash;
	}

	class Q3Data {
		private String tweetTime;
		private int impactScore;
		private String tweetId;
		private String tweetText;

		public Q3Data(String tweetTime, int impactScore, String tweetId, String tweetText) {
			this.tweetTime = tweetTime;
			this.impactScore = impactScore;
			this.tweetId = tweetId;
			this.tweetText = tweetText;
		}

		public String getTweetTime() {
			return tweetTime;
		}

		public int getImpactScore() {
			return impactScore;
		}

		public String getTweetId() {
			return tweetId;
		}

		public String getTweetText() {
			return tweetText;
		}

		public String toString() {
			return tweetTime + "," + impactScore + "," + tweetId + "," + tweetText + "\n";
		}
	}

	class Q3DataComparatorPos implements Comparator<Q3Data> {
		@Override
		public int compare(Q3Data o1, Q3Data o2) {
			// TODO Auto-generated method stub
			if (o1.getImpactScore() == o2.getImpactScore()) {
				return o1.getTweetId().compareTo(o2.getTweetId());
			} else {
				return o2.getImpactScore() - o1.getImpactScore();
			}
		}
	}

	class Q3DataComparatorNeg implements Comparator<Q3Data> {
		@Override
		public int compare(Q3Data o1, Q3Data o2) {
			// TODO Auto-generated method stub
			if (o1.getImpactScore() == o2.getImpactScore()) {
				return o1.getTweetId().compareTo(o2.getTweetId());
			} else {
				return o1.getImpactScore() - o2.getImpactScore();
			}
		}
	}

	class Q6Data {
		private int seq;
		private String tweetid;
		private Semaphore sem;

		public Q6Data(int seq, String tweetid, Semaphore sem) {
			this.seq = seq;
			this.tweetid = tweetid;
			this.sem = sem;
		}

		public String getTweetid() {
			return tweetid;
		}

		public void setTweetid(String tweetid) {
			this.tweetid = tweetid;
		}

		public int getSeq() {
			return seq;
		}

		public void setSeq(int seq) {
			this.seq = seq;
		}

		public Semaphore getSem() {
			return sem;
		}

		public void setSem(Semaphore sem) {
			this.sem = sem;
		}
	}

	class Q6DataComparator implements Comparator<Q6Data> {
		@Override
		public int compare(Q6Data o1, Q6Data o2) {
			return Integer.compare(o1.getSeq(), o2.getSeq());
		}
	}
}