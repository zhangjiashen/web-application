import java.util.Comparator;

class KeyProbability implements Comparator<KeyProbability>, Comparable<KeyProbability>{
	public String key;
	public double probability;
	
	public KeyProbability(String key, double probability){
		this.key = key;
		this.probability = probability;
	}
	@Override
	public int compareTo(KeyProbability o) {
		return Double.compare(o.probability, probability);
	}

	@Override
	public int compare(KeyProbability o1, KeyProbability o2) {
		return Double.compare(o2.probability, o1.probability);
	}
	
	
	
}