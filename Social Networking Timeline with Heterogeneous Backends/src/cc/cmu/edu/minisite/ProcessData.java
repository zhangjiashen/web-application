package cc.cmu.edu.minisite;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;

/* reference:
 * https://www.caveofprogramming.com/java/java-file-reading-and-writing-files-in-java.html
 * http://stackoverflow.com/questions/2885173/how-to-create-a-file-and-write-to-a-file-in-java
 * */
public class ProcessData {

	
	
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException{
        String fileName = "/Users/haoming/Downloads/links.csv";
        String line = null;
        String follower;
        String followee;
        //String[] words;
        HashMap<String, String> followers = new HashMap<String, String>();
        HashMap<String, String> followees = new HashMap<String, String>();
        ArrayList<String> followerList = new ArrayList<String>();
        ArrayList<String> followeeList = new ArrayList<String>();
        String tmp = null;
        try {
            FileReader fileReader = new FileReader(fileName);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
                String[] words = line.split(",");
                followee = words[0];
                follower = words[1];
                if (!followers.containsKey(followee)){
                	followers.put(followee, follower);
                	followeeList.add(followee);
                } else{
                	tmp = followers.get(followee);
                	followers.put(followee, tmp + " " + follower);
                }
                
                if (!followees.containsKey(follower)){
                	followees.put(follower, followee);
                	followerList.add(follower);
                } else{
                	tmp = followees.get(follower);
                	followees.put(follower, tmp + " " + followee);
                }
            
            }
            bufferedReader.close();         
        }
        catch(Exception ex) {
        	ex.printStackTrace();
        }
        
        PrintWriter writer = new PrintWriter("/Users/haoming/Downloads/followers.csv");
        for (String f : followeeList){
        	writer.println(f + "," + followers.get(f));
        }
        writer.close();
        
        writer = new PrintWriter("/Users/haoming/Downloads/followees.csv");
        for (String f : followerList){
        	writer.println(f + "," + followees.get(f));
        }
        writer.close();
	}
}
