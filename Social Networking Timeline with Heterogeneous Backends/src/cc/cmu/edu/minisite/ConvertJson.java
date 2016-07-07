package cc.cmu.edu.minisite;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import org.json.JSONTokener;

import com.amazonaws.util.json.JSONObject;


public class ConvertJson {
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException{
        String fileName = "/Users/haoming/Downloads/posts.json";
        String line = null;
        //String tmp = null;
        StringBuilder sb;
        //JSONTokener tokener;
        int i;
        PrintWriter writer = new PrintWriter("/Users/haoming/Downloads/DDBinput");
        try {
            FileReader fileReader = new FileReader(fileName);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
            	//System.out.println(line);
            	/*
            	tokener = new JSONTokener(line);
            	JSONObject object = new JSONObject(tokener);
            	System.out.println(object.toString());
            	*/
            	sb = new StringBuilder();
            	int index1 = line.indexOf("\"uid\":");
            	int index1end = line.indexOf(',', index1);
            	int index2 = line.indexOf("\"timestamp\":");
            	int index2end = line.indexOf(',', index2) - 1;
            	sb.append("UserID" + ((char)3) + "{\"n\":\"" + line.substring(index1+7, index1end) + "\"}" +
            			((char)2) + "Timestamp" + ((char)3) + "{\"s\":\"" + line.substring(index2+14, index2end) + "\"}");
            	sb.append( "Post" + "{\"s\":\"");
                for (i=0; i < line.length(); i++){
                	if (line.charAt(i) == '"'){
                		sb.append("\\" + "\"");
                	} else {
                		sb.append(line.charAt(i));
                	}
                }
                sb.append("\"}");
                writer.println(sb.toString());
            }
            bufferedReader.close();         
        }
        catch(Exception ex) {
        	ex.printStackTrace();
        }
        
        
        
        writer.close();
	}

}
