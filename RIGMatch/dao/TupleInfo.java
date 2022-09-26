package dao;

import java.util.HashSet;
import query.graph.QEdge;

import java.util.Set;


/*
 * schema information of the tuples
 * 
 */
		
		
public class TupleInfo {
    
	private Set<Integer> nodeIDSet = new HashSet<Integer>();
	
	public TupleInfo(){}
	
	public TupleInfo(Set<Integer> ids) {

		nodeIDSet.addAll(ids);

	}
	
	public TupleInfo(TupleInfo s){
		
		nodeIDSet.addAll(s.nodeIDSet);
	}

	public void addField(int id){
		
		nodeIDSet.add(id);
	}
	
	public void addFields(QEdge e){
		
		nodeIDSet.add(e.from);
		nodeIDSet.add(e.to);
	}
	
	public void addFields(Set<Integer> ids){
		
		nodeIDSet.addAll(ids);
	}
	
	public int getLength(){
		
		return nodeIDSet.size();
	}
	
	public Set<Integer> getNodeIDSet(){
		
		return nodeIDSet;
	}
	
	public boolean isField(int id){
		
		return nodeIDSet.contains(id);
	}
	
	public static void main(String[] args) {

	}

}
