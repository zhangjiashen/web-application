public class CensoredTweet{
		public String timestamp;
		public String user_id;
		public int score;
		public String text;
		public CensoredTweet(String timestamp, String text, String user_id, int score){
			this.timestamp = timestamp;
			this.text = text;
			this.user_id = user_id;
			this.score = score;
		}
		public CensoredTweet(){}
	}