package binaryJoin.bas;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import query.graph.QEdge;

public class State {

	Set<QEdge> edgeSet = new HashSet<QEdge>();
	Set<Integer> nodeSet = new HashSet<Integer>();
	
	double cost;
	int cardinality;
	State inState = null;
	QEdge inEdge  = null; // edge between this state and its parent inState
	
	ArrayList<State> parents= new ArrayList<State>();
	
	public State(){}
	
	public State(Set<QEdge> es){
		
		Iterator<QEdge> it = es.iterator();
		while(it.hasNext()){
			
			addEdge(it.next());
		}
		
	}
	
	
	public void addEdge(QEdge e){
		
		edgeSet.add(e);
		nodeSet.add(e.from);
		nodeSet.add(e.to);
		
	}
	
	public Set<Integer> getNodeSet(){
		
		return nodeSet;
	}
	
	public void addParent(State par){
		
		parents.add(par);
	}

	public void setCard(int card){
		
		cardinality = card;
		
	}
	
	public void setCost(double c){
		
		cost = c;
	}
	
	public void setState(double c, int card, State inS, QEdge inE){
		
		cost = c;
		cardinality = card;
		inState = inS;
		inEdge = inE;
		
	}
	
	public static void main(String[] args) {
		

	}

}
