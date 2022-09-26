package query.graph;

import java.util.ArrayList;
import java.util.Set;



public class Dag2Tree {

	private Query mQuery;
	private int numDeltaEdges = 0;
	private Set<QEdge> delta;

	
	
	public Dag2Tree(Query query) {

		mQuery = query;

	}

	
	public Set<QEdge> getDeltaEdges(){
		
	    return delta;	
	}
	
	public Query genTree() {

		SpanningTreeQuery stq = new SpanningTreeQuery(mQuery);
		delta = stq.run();
		numDeltaEdges = delta.size();
		QNode[] nodes = genNodes(); //get all the nodes of the original query 
	
		QEdge[] edges = new QEdge[mQuery.E-numDeltaEdges]; // remove the non-tree edges
		int i=0;
		//update the nodes
		for(QEdge e: delta){
			QNode v = nodes[e.from], w = nodes[e.to];
			System.out.println(v.id + "->" + w.id);
			//remove it
		
			v.N_O_SZ--;
			w.N_I_SZ--;
			v.N_O.remove(Integer.valueOf(w.id));
			w.N_I.remove(Integer.valueOf(v.id));
			v.E_O.remove(e);
			w.E_I.remove(e);
			
		}
		for(QEdge e: mQuery.edges){
			
			if(!delta.contains(e)){
				
				edges[i++] = e;
			}
		}
		// a new query which has all the nodes of the original query, 
		//but has only the non-tree edges.
		return new Query(mQuery.Qid, nodes, edges); 
	}

	// virtual root of the query
	public QNode genVRoot(){
		QNode n = new QNode();
		n.id =-1;
		n.N_I_SZ = 0;
		ArrayList<QNode> sources = mQuery.sources;
		n.N_O_SZ = sources.size();
		n.E_O = new ArrayList<QEdge>(n.N_O_SZ);
		n.N_O = new ArrayList<Integer>(n.N_O_SZ);
		for(QNode s:sources){
			
			QEdge e = new QEdge(-1, s.id);
			n.E_O.add(e);
			n.N_O.add(s.id);
		}
		
		return n;
	}
	
	private QNode[] genNodes() {

		QNode[] nodes = new QNode[mQuery.V];
		
		for (QNode o : mQuery.nodes) {

			QNode n = new QNode();
			n.id = o.id;
			n.lb = o.lb;
			n.N_O_SZ = o.N_O_SZ;
			n.N_I_SZ = o.N_I_SZ;
			if (n.N_O_SZ > 0) {
				n.N_O = new ArrayList<Integer>(n.N_O_SZ);
				for(int i:o.N_O)
					n.N_O.add(i);
				n.E_O = new ArrayList<QEdge>(n.N_O_SZ);
				for(QEdge e:o.E_O)
					n.E_O.add(e);
			}
			
			if (n.N_I_SZ > 0) {
				n.N_I = new ArrayList<Integer>(n.N_I_SZ);
				for(int i:o.N_I)
					n.N_I.add(i);
				n.E_I = new ArrayList<QEdge>(n.N_I_SZ);
				for(QEdge e:o.E_I)
					n.E_I.add(e);
			}
			
			nodes[n.id] = n; 
		}
		
		return nodes;
	}
	

	public static void main(String[] args) {

	}

}
