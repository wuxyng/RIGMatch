/*
 * Compute preorder and postorder for a digraph 
 * 
 */

package query.graph;

import edu.princeton.cs.algs4.Queue;
import edu.princeton.cs.algs4.Stack;
import edu.princeton.cs.algs4.StdOut;

public class DepthFirstOrder {

	private boolean[] marked; // marked[v] = has v been marked in dfs?
	private int[] pre; // pre[v] = preorder number of v
	private int[] post; // post[v] = postorder number of v
	private Queue<Integer> preorder; // vertices in preorder
	private Queue<Integer> postorder; // vertices in postorder
	private int preCounter; // counter or preorder numbering
	private int postCounter; // counter for postorder numbering

	public DepthFirstOrder(Query Q) {

		pre = new int[Q.V()];
		post = new int[Q.V()];
		postorder = new Queue<Integer>();
		preorder = new Queue<Integer>();
		marked = new boolean[Q.V()];
		for (int v = 0; v < Q.V(); v++)
			if (!marked[v])
				dfs(Q, v);
	}

	private void dfs(Query Q, int v) {
		marked[v] = true;
		pre[v] = preCounter++;
		preorder.enqueue(v);
		for (int w : Q.getChildrenIDs(v)) {
			if (!marked[w]) {
				dfs(Q, w);
			}
		}
		postorder.enqueue(v);
		post[v] = postCounter++;
	}
	
	
	 /**
     * Returns the preorder number of vertex <tt>v</tt>.
     * @param v the vertex
     * @return the preorder number of vertex <tt>v</tt>
     */
    public int pre(int v) {
        return pre[v];
    }

    /**
     * Returns the postorder number of vertex <tt>v</tt>.
     * @param v the vertex
     * @return the postorder number of vertex <tt>v</tt>
     */
    public int post(int v) {
        return post[v];
    }

    
    /**
     * Returns the vertices in postorder.
     * @return the vertices in postorder, as an iterable of vertices
     */
    public Iterable<Integer> post() {
        return postorder;
    }

    
    /**
     * Returns the vertices in preorder.
     * @return the vertices in preorder, as an iterable of vertices
     */
    public Iterable<Integer> pre() {
        return preorder;
    }

    /**
     * Returns the vertices in reverse postorder.
     * @return the vertices in reverse postorder, as an iterable of vertices
     */
    public Iterable<Integer> reversePost() {
        Stack<Integer> reverse = new Stack<Integer>();
        for (int v : postorder)
            reverse.push(v);
        return reverse;
    }

    
    // check that pre() and post() are consistent with pre(v) and post(v)
    private boolean check(Query Q) {

        // check that post(v) is consistent with post()
        int r = 0;
        for (int v : post()) {
            if (post(v) != r) {
                StdOut.println("post(v) and post() inconsistent");
                return false;
            }
            r++;
        }

        // check that pre(v) is consistent with pre()
        r = 0;
        for (int v : pre()) {
            if (pre(v) != r) {
                StdOut.println("pre(v) and pre() inconsistent");
                return false;
            }
            r++;
        }


        return true;
    }

    
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
