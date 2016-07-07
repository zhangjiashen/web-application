package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;
import org.json.JSONArray;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
/* References:
 * https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/package-summary.html
 *  */

public class FollowerServlet extends HttpServlet {
	//hbase
	static private String masterDNS;
	static private String tableName;
	static private String family;
	static private String column;
	//mySQL
	private static String SqlURL;
	//private static String SqlDriver;
	private static String username;
	private static String password;
	private static String sqlTablename;
	
    public FollowerServlet() {
    	masterDNS = "ec2-54-173-251-175.compute-1.amazonaws.com";
    	tableName = "follower";
    	family = "data";
    	column = "follower";
    	SqlURL = "jdbc:mysql://localhost:3306/users";
    	//SqlDriver = "com.mysql.jdbc.Driver";
    	username = "root";
    	password = "15319project";
    	sqlTablename = "task1";
    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
    	
    	response.setContentType("text/html");
    	                  
        String id = request.getParameter("id");

        JSONObject result = new JSONObject();
        JSONArray followers = new JSONArray();

        Result r = getFollower(id);
        
        ArrayList<String> followerIDs = new ArrayList<String>();
        // add to string list
        String[] words;
        for (KeyValue keyValue : r.list()){
        	String tmp = new String(keyValue.getValue());
        	words = tmp.split(" ");
        	for (String word : words){
        		followerIDs.add(word);
        	}
        }
        
        // retrieve name and profile according to ID
        ArrayList<NameProfile> nameProfiles = new ArrayList<NameProfile>(followerIDs.size());
        for (String followerID : followerIDs){
        	nameProfiles.add(getNameProfile(followerID));
        }
        Collections.sort(nameProfiles);
                
        JSONObject follower;
        for (NameProfile n : nameProfiles){
        	follower = new JSONObject();
        	follower.put("name", n.name);
        	follower.put("profile", n.profile);
        	followers.put(follower);
        }
        result.put("followers", followers);
        // sample code ends

        PrintWriter writer = response.getWriter();
        writer.write(String.format("returnRes(%s)", result.toString()));
        writer.close();
    }
    public Result getFollower(String id) throws IOException{
    	Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", masterDNS);
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.master", masterDNS + ":60000");
        
        
		HTablePool pool = new HTablePool(config, 500);
        
		HTableInterface table = pool.getTable(Bytes.toBytes(tableName));
		try {
			table.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		table = pool.getTable(Bytes.toBytes(tableName));
        Get get = new Get(id.getBytes());
        get.addColumn(family.getBytes(), column.getBytes());
        //get.setMaxVersions();//Get all available versions.
        Result r = table.get(get);
        return r;
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
    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        doGet(request, response);
    }
}

