import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

public class FormatPageRank{
	
	public static void main(String[] args) throws IOException{
		String filename = "/Users/haoming/Documents/15619/project42/totalpagerank";
		FileReader fileReader = new FileReader(filename);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		PrintWriter writer = new PrintWriter("/Users/haoming/Documents/15619/project42/pagerankfile");
		String line;
		while((line = bufferedReader.readLine()) != null) {
			String tmp = line.substring(1, line.length()-1).replace(',', '\t');
            writer.println(tmp);
            writer.flush();
        }
		bufferedReader.close();
		writer.close();
	}
	
}