
package query.graph;

import java.util.ArrayList;
import java.util.Arrays;

import edu.princeton.cs.algs4.Queue;

public class QueryHandler {

	private Queue<Integer> order; // vertices (vid) in order
	private int[] rank;
	private int[] level;

	public QueryHandler() {

	}

	// Compute topological ordering of a DAG using queue-based algorithm.

	public ArrayList<Integer> topologyList(Query Q) {
		ArrayList<Integer> orderList = new ArrayList<Integer>(Q.V);
		topologyQue(Q);
		
		for(Integer q: order){
			
			orderList.add(q);
		}
		
		
		return orderList;
	}
	
	public static QNode findSpanTreeRoot(Query Q){
		
		ArrayList<QNode> sources = Q.getSources();
		
		for (QNode s: sources){
			
			boolean[] marked = new boolean[Q.V];
			Arrays.fill(marked, false);
			dfs(Q, s, marked);
			int count = 0;
			for(boolean m:marked){
				
				if(m)
					count++;
			}
			
			if(count==Q.V)
				return s;
		}
		
		
		
		return null;
	}
	
	
	private static void dfs(Query Q, QNode q, boolean[] marked){
		marked[q.id] = true;
		for (QNode w : Q.getChildren(q.id)) {
			if (!marked[w.id]) {
				dfs(Q, w,marked);
			}
		}
		
	}
	
	public Queue<Integer> topologyQue(Query Q) {

		// indegrees of remaining vertices
		int[] indegree = new int[Q.V()];
		for (int v = 0; v < Q.V(); v++) {
			indegree[v] = Q.indegree(v);
		}

		// initialize
		rank = new int[Q.V()];
		order = new Queue<Integer>();
		QNode[] nodes = Q.nodes;
		int count = 0;

		// initialize queue to contain all vertices with indegree = 0
		Queue<Integer> queue = new Queue<Integer>();
		for (int v = 0; v < Q.V(); v++)
			if (indegree[v] == 0)
				queue.enqueue(v);

		for (int j = 0; !queue.isEmpty(); j++) {
			int v = queue.dequeue();
			order.enqueue(v);
			rank[v] = count++;
			if (nodes[v].N_O_SZ > 0) {
				for (int w : Q.getChildrenIDs(v)) {
					indegree[w]--;
					if (indegree[w] == 0)
						queue.enqueue(w);
				}
			}
		}

		// there is a directed cycle in subgraph of vertices with indegree >= 1.
		if (count != Q.V()) {
			order = null;
		}

		// assert check(Q);

	
		
		return order;
	}

	public ArrayList<Queue<Integer>> level(Query Q) {

		level = new int[Q.V];
		int max = 0;
		for (QNode q : Q.getSources()) {

			level(Q, q.id);
			if (level[q.id] > max)
				max = level[q.id];
		}

		ArrayList<Queue<Integer>> levelList = new ArrayList<Queue<Integer>>(max + 1);
		for (int i = 0; i <= max; i++) {

			levelList.add(new Queue<Integer>());
		}

		for (int v = 0; v < Q.V(); v++) {

			Queue<Integer> list = levelList.get(level[v]);
			list.enqueue(v);

		}

		return levelList;
	}

	private void level(Query Q, int v) {

		QNode q = Q.getNode(v);
		if (q.isSink()) {

			level[v] = 0;
			return;
		}

		int max = -1;
		if (Q.nodes[v].N_O_SZ > 0)
			for (int w : Q.getChildrenIDs(v)) {

				level(Q, w);
				if (max < 1 + level[w])
					max = 1 + level[w];
			}

		level[v] = max;

	}

	/**
	 * Does the digraph have a topological order?
	 * 
	 * @return <tt>true</tt> if the digraph has a topological order (or
	 *         equivalently, if the digraph is a DAG), and <tt>false</tt>
	 *         otherwise
	 */
	public boolean hasOrder() {
		return order != null;
	}

	/**
	 * The the rank of vertex <tt>v</tt> in the topological order; -1 if the
	 * digraph is not a DAG
	 * 
	 * @return the position of vertex <tt>v</tt> in a topological order of the
	 *         digraph; -1 if the digraph is not a DAG
	 * @throws IndexOutOfBoundsException
	 *             unless <tt>v</tt> is between 0 and <em>V</em> &minus; 1
	 */
	public int rank(int v) {
		validateVertex(v);
		if (hasOrder())
			return rank[v];
		else
			return -1;
	}

	/**
	 * Returns a topological order if the digraph has a topologial order, and
	 * <tt>null</tt> otherwise.
	 * 
	 * @return a topological order of the vertices (as an interable) if the
	 *         digraph has a topological order (or equivalently, if the digraph
	 *         is a DAG), and <tt>null</tt> otherwise
	 */
	public Iterable<Integer> order() {
		
		return order;
	}

	// throw an IndexOutOfBoundsException unless 0 <= v < V
	private void validateVertex(int v) {
		int V = rank.length;
		if (v < 0 || v >= V)
			throw new IndexOutOfBoundsException("vertex " + v + " is not between 0 and " + (V - 1));
	}

	// certify that digraph is acyclic
	private boolean check(Query Q) {

		// digraph is acyclic
		if (hasOrder()) {
			// check that ranks are a permutation of 0 to V-1
			boolean[] found = new boolean[Q.V()];
			for (int i = 0; i < Q.V(); i++) {
				found[rank(i)] = true;
			}
			for (int i = 0; i < Q.V(); i++) {
				if (!found[i]) {
					System.err.println("No vertex with rank " + i);
					return false;
				}
			}

			// check that ranks provide a valid topological order
			for (int v = 0; v < Q.V(); v++) {
				for (int w : Q.getChildrenIDs(v)) {
					if (rank(v) > rank(w)) {
						System.err.printf("%d-%d: rank(%d) = %d, rank(%d) = %d\n", v, w, v, rank(v), w, rank(w));
						return false;
					}
				}
			}

			// check that order() is consistent with rank()
			int r = 0;
			for (int v : order()) {
				if (rank(v) != r) {
					System.err.println("order() and rank() inconsistent");
					return false;
				}
				r++;
			}
		}

		return true;
	}

	public static void main(String[] args) {

	}

}
