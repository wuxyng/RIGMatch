package dao;

import java.util.Iterator;
import java.util.Stack;

import global.Consts;
import global.Flags;
import graph.GraphNode;

public class BFLIndex {

	GraphNode[] nodes;

	private Iterator<Integer>[] out_it;
	
	public BFLIndex(GraphNode[] nodes) {

		this.nodes = nodes;
		out_it = (Iterator<Integer>[]) new Iterator[nodes.length];

	}

	public int reach(int u, int v) {

		return reach(nodes[u], nodes[v]);

	}

	public int reach(GraphNode u, GraphNode v) {

		Flags.vis_cur++;
		// return chkReach(u, v) ? 1 : 0;
		return chkReach_iter(u, v) ? 1 : 0;
	}

	public int length() {

		return nodes.length;
	}
	
	public GraphNode[] getGraphNodes(){
		
		return nodes;
	}

	private boolean chkReach_iter(GraphNode s, GraphNode t) {

		int rs = chkReach_base(s, t);
		if (rs == 1)
			return true;
		else if (rs == 0)
			return false;
		// otherwise undetermined
		Stack<GraphNode> stack = new Stack<GraphNode>();
		stack.push(s);
		s.vis = Flags.vis_cur;
		if (s.N_O_SZ > 0)
			out_it[s.id] = s.N_O.iterator();

		while (!stack.isEmpty()) {
			GraphNode u = stack.peek();
			if (out_it[u.id].hasNext()) {
				GraphNode v = nodes[out_it[u.id].next()];
				if (v.vis != Flags.vis_cur) {
					v.vis = Flags.vis_cur; // should set it here
					rs = chkReach_base(v, t);
					if (rs == 1)
						return true;
					// cannot return false when rs == 0, since it is possible
					// that another child node v of u can reach t.

					if (rs == -1) {
						if (v.N_O_SZ > 0)
							out_it[v.id] = v.N_O.iterator();

						stack.push(v);
					}
				}
				continue;
			}

			stack.pop();

		}

		return false;
	}

	private int chkReach_base(GraphNode u, GraphNode v) {

		// 0: false, 1: true

		// from u to v, u starts earlier than v
		if (u.L_interval.mEnd < v.L_interval.mEnd) {
			return 0;
			// must <=
		} else if (u.L_interval.mStart <= v.L_interval.mStart) {
			return 1;
		}

		if (v.N_I_SZ == 0) {
			return 0;
		}
		if (u.N_O_SZ == 0) {
			return 0;
		}
		if (v.N_O_SZ == 0) {

			int u_h_in = ((GraphNode) v).h_out & 0x0FFFFFFFF;
			if ((u.L_out[u_h_in >>> 5] & (1 << (((GraphNode) v).h_out & 31))) == 0)
				return 0;

		} else {
			for (int i = 0; i < Consts.K; i++) {
				if ((u.L_out[i] & v.L_out[i]) != v.L_out[i]) {
					return 0;
				}
			}
		}

		if (u.N_I_SZ == 0) {
			int u_h_in = ((GraphNode) u).h_in & 0x0FFFFFFFF;
			if ((v.L_in[u_h_in >>> 5] & (1 << (((GraphNode) u).h_in & 31))) == 0)
				return 0;

		} else {
			for (int i = 0; i < Consts.K; i++) {
				if ((u.L_in[i] & v.L_in[i]) != u.L_in[i]) {
					return 0;
				}
			}
		}

		return -1; // undetermined

	}

	private boolean chkReach(GraphNode u, GraphNode v) {

		// from u to v, u starts earlier than v
		if (u.L_interval.mEnd < v.L_interval.mEnd) {
			return false;
			// must <=
		} else if (u.L_interval.mStart <= v.L_interval.mStart) {
			return true;
		}

		if (v.N_I_SZ == 0) {
			return false;
		}
		if (u.N_O_SZ == 0) {
			return false;
		}
		if (v.N_O_SZ == 0) {

			int u_h_in = ((GraphNode) v).h_out & 0x0FFFFFFFF;
			if ((u.L_out[u_h_in >>> 5] & (1 << (((GraphNode) v).h_out & 31))) == 0)
				return false;

		} else {
			for (int i = 0; i < Consts.K; i++) {
				if ((u.L_out[i] & v.L_out[i]) != v.L_out[i]) {
					return false;
				}
			}
		}

		if (u.N_I_SZ == 0) {
			int u_h_in = ((GraphNode) u).h_in & 0x0FFFFFFFF;
			if ((v.L_in[u_h_in >>> 5] & (1 << (((GraphNode) u).h_in & 31))) == 0)
				return false;

		} else {
			for (int i = 0; i < Consts.K; i++) {
				if ((u.L_in[i] & v.L_in[i]) != u.L_in[i]) {
					return false;
				}
			}
		}

		for (int i = 0; i < u.N_O_SZ; i++) {
			if (nodes[u.N_O.get(i)].vis != Flags.vis_cur) {
				nodes[u.N_O.get(i)].vis = Flags.vis_cur;
				if (chkReach(nodes[u.N_O.get(i)], v)) {
					return true;
				}
			}
		}

		return false;

	}

	public static void main(String[] args) {

	}

}
