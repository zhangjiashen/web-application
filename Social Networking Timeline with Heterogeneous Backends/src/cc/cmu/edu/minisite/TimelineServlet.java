package cc.cmu.edu.minisite;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;

import cc.cmu.edu.minisite.HomepageServlet.Homepage;
import cc.cmu.edu.minisite.HomepageServlet.HomepageCompare;

public class TimelineServlet extends HttpServlet {
	//hbase
	static private String masterDNS;
	static private String tableNameFollower;
	static private String tableNameFollowee;
	static private String family;
	static private String column1;
	static private String column2;
	//mySQL
	private static String SqlURL;
	private static String sqlTablename;
	//private static String SqlDriver;
	private static String username;
	private static String password;
	
	//DynamoDB
	static AmazonDynamoDBClient client;
	//private Properties properties;
	static BasicAWSCredentials bawsc;
    public TimelineServlet() throws Exception {

    	masterDNS = "ec2-54-173-251-175.compute-1.amazonaws.com";
    	tableNameFollower = "follower";
    	tableNameFollowee = "followee";
    	family = "data";
    	column1 = "follower";
    	column2 = "followee";
    	SqlURL = "jdbc:mysql://localhost:3306/users";
    	//SqlDriver = "com.mysql.jdbc.Driver";
    	username = "root";
    	password = "15319project";
    	sqlTablename = "task1";
    	String accessKey;
		String secretKey;
		
        FileReader fileReader1 = new FileReader("/home/ubuntu/Project3_4/accessKey");
        FileReader fileReader2 = new FileReader("/home/ubuntu/Project3_4/secretKey");

        BufferedReader bufferedReader1 = new BufferedReader(fileReader1);
        BufferedReader bufferedReader2 = new BufferedReader(fileReader2);
        accessKey = bufferedReader1.readLine();
		secretKey = bufferedReader2.readLine();
		bufferedReader1.close();
		bufferedReader2.close();
		fileReader1.close();
		fileReader2.close();
		bawsc = new BasicAWSCredentials(accessKey, secretKey);
    	client = new AmazonDynamoDBClient(bawsc);
    }

    @Override
    protected void doGet(final HttpServletRequest request, 
            final HttpServletResponse response) throws ServletException, IOException {

    	response.setContentType("text/html");
    	String uid = request.getParameter("id");
    	
        JSONObject result = new JSONObject();
        
        
        ArrayList<Homepage> homepages = new ArrayList<Homepage>();
        
        // build DDB mapper
        DynamoDBMapper mapper = null;
        try {
            mapper = new DynamoDBMapper(client);
            
        } catch (Throwable t) {
            t.printStackTrace();
        }

        //get name and profile
        NameProfile myNameProfile = getNameProfile(uid);
        
        //get followers
        Result followerResult = getFollow(uid, tableNameFollower, column1);
        ArrayList<String> followerIDs = new ArrayList<String>();
        String[] words;
        for (KeyValue keyValue : followerResult.list()){
        	String tmp = new String(keyValue.getValue());
        	words = tmp.split(" ");
        	for (String word : words){
        		followerIDs.add(word);
        	}
        }
        // retrieve name and profile according to ID
        ArrayList<NameProfile> followerNameProfiles = new ArrayList<NameProfile>(followerIDs.size());
        for (String followerID : followerIDs){
        	followerNameProfiles.add(getNameProfile(followerID));
        }
        Collections.sort(followerNameProfiles);
        
        
        //get followee
        Result followeeResult = getFollow(uid, tableNameFollowee, column2);
        ArrayList<String> followeeIDs = new ArrayList<String>();
        
        for (KeyValue keyValue : followeeResult.list()){
        	String tmp = new String(keyValue.getValue());
        	words = tmp.split(" ");
        	for (String word : words){
        		followeeIDs.add(word);
        		
        		//debug
        		//System.out.println(word + " " + getNameProfile(word).name);
        	}
        }
        
        //get followee posts
        for (String followeeID : followeeIDs){        
        	try {
    			homepages.addAll(findPosts(mapper, followeeID));
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
        }
        
        //get latest followee posts
        Collections.sort(homepages, new LatestHomepage());
        //debug
        /*
        for (Homepage h : homepages){
        	System.out.println(h.getTimestamp());
        }
        */
        //if (homepages.size() > 30)
        homepages = new ArrayList<Homepage>(homepages.subList(0, 30));
        
        /* build response */
        
        
        
        //this user
        result.put("name", myNameProfile.name);
        result.put("profile", myNameProfile.profile);
        
        //followers
        JSONArray followers = new JSONArray();
        JSONObject follower;
        for (NameProfile n : followerNameProfiles){
        	follower = new JSONObject();
        	follower.put("name", n.name);
        	follower.put("profile", n.profile);
        	followers.put(follower);
        }
        result.put("followers", followers);
        
        // posts
        StringBuilder sb = new StringBuilder(result.toString());
        sb.setCharAt(sb.length()-1, ',');
        sb.append(" \"posts\":[");
        //for (int i=0; i < homepages.size(); i++){
        for (int i = (homepages.size() - 1) ; i >= 0; i--){
        	Homepage homepage = homepages.get(i);
        	if (i != homepages.size() - 1){
        		sb.append(',');
        	}
        	sb.append(homepage.getPost());
        }
        sb.append("]}"); 
        
        

        PrintWriter out = response.getWriter();
        out.print(String.format("returnRes(%s)", sb.toString()));
        out.close();
    }

    @Override
    protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
    }
    
    public Result getFollow(String id, String tableName, String column) throws IOException{
    	Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", masterDNS);
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.master", masterDNS + ":60000");
        
        
		HTablePool pool = new HTablePool(config, 500);
        
		HTableInterface table = pool.getTable(Bytes.toBytes(tableName));
        Get get = new Get(id.getBytes());
        get.addColumn(family.getBytes(), column.getBytes());
        //get.setMaxVersions();//Get all available versions.
        Result r = table.get(get);
        table.close();
        return r;
    }
    
    
    
    private static List<Homepage> findPosts(DynamoDBMapper mapper, String uid) throws Exception {
        
        int hashKey = Integer.parseInt(uid);
        
        Homepage homepage = new Homepage();
        homepage.setUserID(hashKey);
        
        DynamoDBQueryExpression<Homepage> queryExpression = new DynamoDBQueryExpression<Homepage>()
            .withHashKeyValues(homepage);

        List<Homepage> homePages = mapper.query(Homepage.class, queryExpression);
        
        return homePages;
       }
    

    public NameProfile getNameProfile(String id){
    	String name = null;
        String profile = null;
        Statement statement = null;
        ResultSet rs = null;
        Connection conn = null;
        try {
        	// Register JDBC driver
        	Class.forName("com.mysql.jdbc.Driver");

        	// Open a connection
        	conn = DriverManager.getConnection(SqlURL, username, password);

        	// Execute SQL query
        	statement = conn.createStatement();
        	String sql;
        	sql = "SELECT name, profile FROM " + sqlTablename + " WHERE uid=\"" + id + "\"";
        	rs = statement.executeQuery(sql);
        
        	// Extract data from result set
        	while(rs.next()){
        		//Retrieve by column name
        		name = rs.getString("name");
        		profile = rs.getString("profile");

        		//Display values
        		//System.out.println("Name: " + name);
        		//System.out.println("Profile: " + profile);
        	}

        } catch (Exception e) {
        	e.printStackTrace();
        } finally{
            try {
    			if (rs != null)
    				rs.close();
    		} catch (SQLException e) {
    			e.printStackTrace();
    		}
        	try {
        		if(statement != null)
    				statement.close();
    		} catch (SQLException e) {
    			e.printStackTrace();
    		}
        	try {
        		if (conn != null)
    				conn.close();
    		} catch (SQLException e) {
    			e.printStackTrace();
    		}

        }
        if (name == null){
        	name = "Unauthorized";
        }
        if (profile == null){
        	profile = "#";
        }
    	return new NameProfile(name, profile,id);
    }
    
    
    public class LatestHomepage implements Comparator<Homepage>{

		@Override
		public int compare(Homepage o1, Homepage o2) {
			return o2.getTimestamp().compareTo(o1.getTimestamp());
		}
    	
    }
}
