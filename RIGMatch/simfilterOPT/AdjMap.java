package simfilterOPT;

import java.util.ArrayList;
import java.util.HashMap;

public class AdjMap {
	HashMap<Integer, ArrayList<Integer>> adjmap;

	public AdjMap(int size) {

		adjmap = new HashMap<Integer, ArrayList<Integer>>(size);

	}

	void addValue(int k, int v) {
		ArrayList<Integer> vals = adjmap.get(k);
		if (vals == null) {

			vals = new ArrayList<Integer>();
			adjmap.put(k, vals);
		}

		//vals.add(v);
		vals.add(0,v);
		
	}

	ArrayList<Integer> getValue(int k) {

		return adjmap.get(k);
	}

	void clear(int k) {

		adjmap.put(k, null);
	}
	
	void clear(int k, int v){
		ArrayList<Integer> vals = adjmap.get(k);
		if(vals.size()>1)
			vals.remove(Integer.valueOf(v));
		else
			adjmap.put(k, null);
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
