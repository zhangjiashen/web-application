package cc.cmu.edu.minisite;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.util.json.JSONTokener;

/* References:
 * http://stackoverflow.com/questions/333363/loading-a-properties-file-from-java-package
 * */
public class HomepageServlet extends HttpServlet {

	static AmazonDynamoDBClient client;
	//private Properties properties;
	static BasicAWSCredentials bawsc;
    public HomepageServlet() throws IOException {
		//properties = new Properties();
		/*
		try {
			properties.load(getClass().getResourceAsStream("/home/ubuntu/Project3_4/AwsCredentials.properties"));
		} catch (IOException e) {
			System.out.println("error loading AWS credential");
			//e.printStackTrace();
		}
		*/
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
        JSONArray posts = new JSONArray();
        //JSONObject result = new JSONObject();
        StringBuilder result = new StringBuilder();
        result.append("{\"posts\":[");
        List<Homepage> homepages = null;
        try {
            DynamoDBMapper mapper = new DynamoDBMapper(client);
            homepages = findPosts(mapper, uid);
            //Collections.sort(homepages, new HomepageCompare());
        } catch (Throwable t) {
            t.printStackTrace();
        }
        //System.out.println(homepages.size());
        for (int i=0; i < homepages.size(); i++){
        	Homepage homepage = homepages.get(i);
        	if (i != 0){
        		result.append(',');
        	}
        	result.append(homepage.getPost());
        	//System.out.println(tmp);
        	
        }
        result.append("]}");           
        //System.out.println(result.toString());
        PrintWriter writer = response.getWriter();
        writer.write(String.format("returnRes(%s)", result.toString()));
        writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest request, 
            final HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
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

/*
    public class Post{
            private String UserID;
            private String name;
            private String profile;
            private String Timestamp;
            private String image;
            private String content;
            private List<Comment> comments;
            
    }
  */  
    
    @DynamoDBTable(tableName="task3")
    public static class Homepage {
        private int UserID;
        /*
        private String name;
        private String profile;
        */
        private String Timestamp;
        private String Post;
        /*
        private String image;
        private String content;
        private List<Comment> comments;
        */
        @DynamoDBHashKey(attributeName="UserID")
        public int getUserID() { return UserID; }
        public void setUserID(int UserID) { this.UserID = UserID; }
        
        @DynamoDBRangeKey(attributeName="Timestamp")
        public String getTimestamp() { return Timestamp; }
        public void setTimestamp(String Timestamp) { this.Timestamp = Timestamp; }
        
        @DynamoDBAttribute(attributeName="Post")
		public String getPost() {
			return Post;
		}
		public void setPost(String post) {
			Post = post;
		}
        
        //
        
        /*
        @DynamoDBHashKey(attributeName="pid")
        public String getPid() { return pid; }
        public void setPid(String pid) { this.pid = pid; }
        

        
        @DynamoDBHashKey(attributeName="name")
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        
        @DynamoDBHashKey(attributeName="profile")
        public String getProfile() { return profile; }
        public void setProfile(String profile) { this.profile = profile; }
        
        @DynamoDBHashKey(attributeName="timestamp")
        public String getTimestamp() { return timestamp; }
        public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
        
        @DynamoDBHashKey(attributeName="image")
        public String getImage() { return image; }
        public void setImage(String image) { this.image = image; }
        
        @DynamoDBHashKey(attributeName="content")
        public String getContent() { return content; }
        public void setContent(String content) { this.content = content; }
        
        @DynamoDBHashKey(attributeName="comments")
        public List<Comment> getComments() { return comments; }
        public void setComments(List<Comment> comments) { this.comments = comments; }
        */
    }
    

    public class HomepageCompare implements Comparator<Homepage>{

		@Override
		public int compare(Homepage o1, Homepage o2) {
			return o1.getTimestamp().compareTo(o2.getTimestamp());
		}
    	
    }

}
