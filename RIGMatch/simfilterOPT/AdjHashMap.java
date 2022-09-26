package simfilterOPT;

import java.util.ArrayList;
import java.util.HashMap;

public class AdjHashMap {
	HashMap<Integer, Integer> adjmap;

	public AdjHashMap(int size) {

		adjmap = new HashMap<Integer, Integer>(size);

	}

	void addValue(int k, int v) {
		
		adjmap.put(k, v);
	}

	Integer getValue(int k) {

		return adjmap.get(k);
	}

	void clear(int k) {

		adjmap.put(k, null);
	}
	

	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
