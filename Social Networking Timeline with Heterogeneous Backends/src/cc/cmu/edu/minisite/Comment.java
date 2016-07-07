package cc.cmu.edu.minisite;
public class Comment{
        	private String uid;
            private String name;
            private String profile;
            private String timestamp;
            private String content;
			public String getUid() {
				return uid;
			}
			public void setUid(String uid) {
				this.uid = uid;
			}
			public String getName() {
				return name;
			}
			public void setName(String name) {
				this.name = name;
			}
			public String getProfile() {
				return profile;
			}
			public void setProfile(String profile) {
				this.profile = profile;
			}
			public String getTimestamp() {
				return timestamp;
			}
			public void setTimestamp(String timestamp) {
				this.timestamp = timestamp;
			}
			public String getContent() {
				return content;
			}
			public void setContent(String content) {
				this.content = content;
			}
        }