package query.graph;

import edu.princeton.cs.algs4.Stack;

public class QueryDirectedCycle {

	private boolean[] marked; // marked[v] = has vertex v been marked?
	private int[] edgeTo; // edgeTo[v] = previous vertex on path to v
	private boolean[] onStack; // onStack[v] = is vertex on the stack?
	private Stack<Integer> cycle; // directed cycle (or null if no such cycle)

	public QueryDirectedCycle(Query Q) {

		marked = new boolean[Q.V()];
		onStack = new boolean[Q.V()];
		edgeTo = new int[Q.V()];
		for (int v = 0; v < Q.V(); v++)
			if (!marked[v] && cycle == null)
				dfs(Q, v);
	}

	private void dfs(Query Q, int v) {

		onStack[v] = true;
		marked[v] = true;
		if (Q.nodes[v].N_O_SZ > 0)
			for (int w : Q.getChildrenIDs(v)) {

				// short circuit if directed cycle found
				if (cycle != null)
					return;

				// found new vertex, so recur
				else if (!marked[w]) {
					edgeTo[w] = v;
					dfs(Q, w);
				}

				else if (onStack[w]) {
					cycle = new Stack<Integer>();
					for (int x = v; x != w; x = edgeTo[x]) {
						cycle.push(x);
					}
					cycle.push(w);
					cycle.push(v);
					// assert check();
				}

			}

		onStack[v] = false;
	}

	/**
	 * Does the digraph have a directed cycle?
	 * 
	 * @return <tt>true</tt> if the digraph has a directed cycle, <tt>false</tt>
	 *         otherwise
	 */
	public boolean hasCycle() {
		return cycle != null;
	}

	/**
	 * Returns a directed cycle if the digraph has a directed cycle, and
	 * <tt>null</tt> otherwise.
	 * 
	 * @return a directed cycle (as an iterable) if the digraph has a directed
	 *         cycle, and <tt>null</tt> otherwise
	 */
	public Iterable<Integer> cycle() {
		return cycle;
	}

	// certify that digraph has a directed cycle if it reports one
	private boolean check() {

		if (hasCycle()) {
			// verify cycle
			int first = -1, last = -1;
			for (int v : cycle()) {
				if (first == -1)
					first = v;
				last = v;
			}
			if (first != last) {
				System.err.printf("cycle begins with %d and ends with %d\n", first, last);
				return false;
			}
		}

		return true;
	}

	public static void main(String[] args) {

	}

}
