package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.sql.*;
import java.util.ArrayList;

import org.json.JSONObject;
import org.json.JSONArray;

/* References:
 *  http://www.tutorialspoint.com/servlets/servlets-database-access.htm
 *  */
public class ProfileServlet extends HttpServlet {

	private static String SqlURL;
	//private static String SqlDriver;
	private static String username;
	private static String password;
	private static String dns;
	private static String table;
    public ProfileServlet() {
    	SqlURL = "jdbc:mysql://localhost:3306/users";
    	//SqlDriver = "com.mysql.jdbc.Driver";
    	username = "root";
    	password = "15319project";
    	dns = "";
    	table = "task1";

    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response) 
            throws ServletException, IOException {
    	
    	response.setContentType("text/html");
    	
        /* JSON sample */
        JSONObject result = new JSONObject();
                  
        PrintWriter writer = response.getWriter();
        String id = request.getParameter("id");
        String pwd = request.getParameter("pwd");
        
                     
        NameProfile nameProfile = getNameProfile(id,pwd);
        
        result.put("name", nameProfile.name);
        result.put("profile", nameProfile.profile);
        // sample code ends

        
        writer.write(String.format("returnRes(%s)", result.toString()));
        writer.close();
    }
    
    public NameProfile getNameProfile(String id, String pwd){
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
        	sql = "SELECT * FROM " + table + " WHERE uid=\"" + id + "\" AND password=\""+ pwd + "\"";
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
    	return new NameProfile(name, profile, id);
    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response) 
            throws ServletException, IOException {
        doGet(request, response);
    }
}
