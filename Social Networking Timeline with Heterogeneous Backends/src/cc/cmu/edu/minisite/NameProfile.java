package cc.cmu.edu.minisite;

import java.util.Comparator;

/* References:
 * http://www.tutorialspoint.com/java/java_using_comparator.htm
 * */
public class NameProfile implements Comparator<NameProfile>, Comparable<NameProfile>{
	public String name;
	public String profile;
	public String id;
	public NameProfile(String n, String p, String i){
		name = n;
		profile = p;
		id = i;
	}
	public int compareTo(NameProfile n){
	      int tmp = (this.name).compareTo(n.name);
	      if (tmp == 0){
	    	  tmp = (this.profile).compareTo(n.profile);
	      }
	      return tmp;
	}
	
	public int compare(NameProfile n, NameProfile m){
	      int tmp = (n.name).compareTo(m.name);
	      if (tmp == 0){
	    	  tmp = (n.profile).compareTo(m.profile);
	      }
	      return tmp;
	}

}