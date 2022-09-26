package query.graph;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import edu.princeton.cs.algs4.Queue;

public class SpanningTreeQuery {

	Query mGraphQuery;

	public SpanningTreeQuery(Query g) {

		mGraphQuery = g;
	}

	public Set<QEdge> run() {

		ArrayList<QNode> sources = mGraphQuery.getSources();
		Queue<QNode> queue = new Queue<QNode>();
		boolean[] marked = new boolean[mGraphQuery.V];
		Set<QEdge> delta = new HashSet<QEdge>(); // non-tree edge
		
		QNode[] nodes = mGraphQuery.nodes;
		
		for (QNode s : sources) {

			marked[s.id] = true;
			queue.enqueue(s);
			
		}
		
		while(!queue.isEmpty()){
			
			QNode q = queue.dequeue();
			ArrayList<QEdge> children = q.E_O;
			if(children!=null){
				for(QEdge e:children){
					int c = e.to;
					if(!marked[c]){
						queue.enqueue(nodes[c]);
						marked[c] = true;
					}
					else
						delta.add(e);
				}
				
			}
		}
		
		return delta;
	}

	public static void main(String[] args) {

	}

}
