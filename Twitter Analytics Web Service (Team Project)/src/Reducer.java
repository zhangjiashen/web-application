package twitterReducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;

import com.google.gson.Gson;


public class Reducer {

	
	
	public static void main(String args[]) {

		Gson gson = new Gson();
		StringReader sr;
		CensoredTweet censoredTweet;
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			// Initialize Variables
			String input;
			String twitterId = null;
			String currentId = null;

			// While we have input on stdin
			while ((input = br.readLine()) != null) {
				try {
					String[] parts = input.split("\t");
					if (parts.length < 2) continue;
					twitterId = parts[0];

					// We have sorted input, so check if we
					// are we on the same word?
					if (currentId != null && currentId.equals(twitterId))
						continue;
					else // The word has changed
					{
						sr = new StringReader(parts[1]);
						censoredTweet = gson.fromJson(sr, CensoredTweet.class);
						System.out.println(twitterId + "\t" + censoredTweet.score + "\t" + censoredTweet.timestamp + "\t" + censoredTweet.user_id + "\t" + censoredTweet.text);
						currentId = twitterId;
					}
				} catch (NumberFormatException e) {
					continue;
				}
			}
			
			

			// // Print out last word if missed
			// if (currentId != null && currentId.equals(twitterId)) {
			// System.out.println(currentId + "\t" + result);
			// }

		} catch (IOException io) {
			io.printStackTrace();
		}
	}
}