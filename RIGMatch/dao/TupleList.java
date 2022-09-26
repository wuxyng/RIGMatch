package dao;

import java.util.LinkedList;

public class TupleList {

	protected LinkedList<Tuple> m_tuples;

	protected TupleInfo m_info;

	/**
	 * Create a new, empty DefaultTupleSet.
	 */

	public TupleList() {

		m_tuples = new LinkedList<Tuple>();
	}

	public TupleList(TupleInfo info){
		
		m_tuples = new LinkedList<Tuple>();
		m_info   = info;
	}
	
	public LinkedList<Tuple> getList(){
		
	    return 	m_tuples;
	}
	
	public TupleInfo getSchema(){
		
		return m_info;
	}
	
	public void addTuple(Tuple t) {

		m_tuples.add(t);

	}

	public void removeTuple(Tuple t) {

		m_tuples.remove(t);
	}


	public int cardinality(){
		
	    return m_tuples.size();	
	}
	
	public void clear() {

		m_tuples.clear();
	}
	
	public static void main(String[] args) {

	}

}
