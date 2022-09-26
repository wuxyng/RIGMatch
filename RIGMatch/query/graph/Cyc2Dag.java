package query.graph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.ListIterator;
import java.util.Stack;

import global.Consts.Color;

public class Cyc2Dag {

	private Query mQuery;
	private Color[] color;
	private ListIterator<Integer>[] out_it;

	private int numBackEdges = 0;
	private HashSet<QEdge> backEdges;

	public Cyc2Dag(Query query) {

		mQuery = query;

	}

	
	public HashSet<QEdge> getBackEdges(){
		
	    return backEdges;	
	}
	
	public Query genDag() {

		color = new Color[mQuery.V];
		Arrays.fill(color, Color.white);
		backEdges = new HashSet<QEdge>(mQuery.V);
		out_it = (ListIterator<Integer>[]) new ListIterator[mQuery.V];
		QNode[] nodes = mQuery.nodes;
		for (int v = 0; v < mQuery.V; v++) {
			QNode node = nodes[v];
			if (node.N_O != null)
				out_it[v] = node.N_O.listIterator();
		}

		nodes = genNodes(); 
		System.out.println("checking back edges...");
		for (QNode n : nodes) {
			if (color[n.id] == Color.white)
				dfs(n, nodes);

		}
		
		QEdge[] edges = new QEdge[mQuery.E-numBackEdges];
		int i=0;
		for(QEdge e: mQuery.edges){
			
			if(!backEdges.contains(e)){
				
				edges[i++] = e;
			}
		}
		
		return new Query(mQuery.Qid, nodes, edges);
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
	private void dfs(QNode u, QNode[] nodes) {

		Stack<QNode> stack = new Stack<QNode>();
		stack.push(u);
		color[u.id] = Color.grey;
		while (!stack.isEmpty()) {

			QNode v = stack.peek();

			if (v.N_O_SZ == 0) {
				color[v.id] = Color.black;
				stack.pop();
			} else {
				if (out_it[v.id].hasNext()) {

					QNode w = nodes[out_it[v.id].next()];
					QEdge e = mQuery.getEdge(v.id, w.id);
					switch (color[w.id]) {

					case white:

						color[w.id] = Color.grey;
						stack.push(w);
						
						break;

					case grey:
						// back edge
						// System.out.println("CYCLE DETECTED!!!");
						System.out.println(v.id + "->" + w.id);
						//remove it
					
						v.N_O_SZ--;
						w.N_I_SZ--;
						v.N_O.remove(Integer.valueOf(w.id));
						w.N_I.remove(Integer.valueOf(v.id));
						v.E_O.remove(e);
						w.E_I.remove(e);
						
						backEdges.add(e);
						numBackEdges++;
					case black:
						break;
					}

					continue;

				}
				color[v.id] = Color.black;
				stack.pop();
			}
		}

	
		
	}

	public static void main(String[] args) {

	}

}
