package dao;

import java.util.ArrayList;
import java.util.Hashtable;

public class TupleHash {

	Hashtable<Integer, ArrayList<Tuple>> ht;
	
	
	public TupleHash(){
		
		ht = new Hashtable<Integer, ArrayList<Tuple>>();
	}
	
	
	public void insert(int k, Tuple t){
		ArrayList<Tuple> vals = ht.get(k);
		if(vals==null){
		   vals = new ArrayList<Tuple>();
		   ht.put(k, vals);
		}
		
		vals.add(t);
		
	}
	
	public void insert(int k, ArrayList<Tuple> list){
		
		ht.put(k, list);
	}
	
	public ArrayList<Tuple> getList(int k){
		
		return ht.get(k);
	}
	
	public static void main(String[] args) {
		

	}

}
