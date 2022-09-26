package dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Stack;
import java.util.concurrent.ThreadLocalRandom;

import org.roaringbitmap.RoaringBitmap;

import global.Consts;
import global.Flags;
import graph.Digraph;
import graph.GraphNode;
import helper.TimeTracker;

public class BFLIndexBuilder {

	private Digraph mG;
	private ArrayList<ArrayList<GraphNode>> mInvLsts;
	private GraphNode[] nodes;
    private int cur;
  	private static int c_i = 0, r_i;
	private static int c_o = 0, r_o;

	private Iterator<Integer>[] in_it, out_it;

	private int[] lbCounters;

	private double bldTime = 0.0;

	public BFLIndexBuilder(Digraph g) {
		mG = g;
		init();
	}

	public void run() {

		TimeTracker tt = new TimeTracker();
		tt.Start();
		Flags.vis_cur++;

		for (GraphNode node : mG.getSinks()) {

			dfs_in_iter(node);

		}

		Flags.vis_cur++;

		cur = 0;

		for (GraphNode node : mG.getSources()) {

			dfs_out_iter(node);
		}

		bldTime = tt.Stop() / 1000;

		populateNodeADJBits();

		System.out.println("Time for building index:" + bldTime + "sec.");
		// resetVisFlag();
	}

	// call this method after the Interval index is obtained
	private void populateNodeADJBits() {

		GraphNode[] nodes = mG.getNodes();
		for (GraphNode node : nodes) {
			node.adj_bits_o = new RoaringBitmap();
			if (node.N_O_SZ > 0)
				for (int id : node.N_O) {

					node.adj_bits_o.add(nodes[id].L_interval.mStart);
				}

			node.adj_bits_i = new RoaringBitmap();

			if (node.N_I_SZ > 0)
				for (int id : node.N_I) {

					node.adj_bits_i.add(nodes[id].L_interval.mStart);
				}

		}

	}

	public double getBldTime() {

		return bldTime;
	}

	public ArrayList<ArrayList<GraphNode>> getInvLsts() {

		return mInvLsts;
	}
	
	

	// iterative dfs
	private void dfs_out_iter(GraphNode u) {

		Stack<GraphNode> stack = new Stack<GraphNode>();
		stack.push(u);
		u.vis = Flags.vis_cur;

		// previsit_out(u);

		if (u.N_O_SZ > 0) {
			// u.N_O_it = u.N_O.iterator();

			for (int i = 0; i < Consts.K; i++) {
				u.L_out[i] = 0;
			}
		}

		u.L_interval.mStart = ++cur;// cur++;
		u.pos = lbCounters[u.lb]++;
		ArrayList<GraphNode> invLst = mInvLsts.get(u.lb);
		invLst.add(u);

		// System.out.println("OUT visiting " + u.id);
		while (!stack.isEmpty()) {
			GraphNode v = stack.peek();

			if (v.N_O_SZ == 0) {
				((GraphNode) v).h_out = (h_out() % (Consts.K * 32));
				stack.pop();
				v.L_interval.mEnd = ++cur;// cur;

			} else {

				// if (v.N_O_it.hasNext()) {
				// GraphNode w = nodes.get(v.N_O_it.next());
				if (out_it[v.id].hasNext()) {
					GraphNode w = nodes[out_it[v.id].next()];
					if (w.vis != Flags.vis_cur) {
						// dfs_out(v);
						w.vis = Flags.vis_cur;
						stack.push(w);
						// previsit_out(w);

						if (w.N_O_SZ > 0) {
							// w.N_O_it = w.N_O.iterator();
							for (int i = 0; i < Consts.K; i++) {
								w.L_out[i] = 0;
							}
						}

						w.L_interval.mStart = ++cur;// cur++;
						w.pos = lbCounters[w.lb]++;
						invLst = mInvLsts.get(w.lb);
						invLst.add(w);
						// System.out.println("OUT visiting " + w.id);

					}

					continue;
				}
				for (int i = 0; i < v.N_O_SZ; i++) {
					GraphNode w = nodes[v.N_O.get(i)];
					if (w.N_O_SZ == 0) {
						int hu = 0;
						hu = ((GraphNode) w).h_out;
						v.L_out[(hu >>> 5) % Consts.K] |= 1 << (hu & 31);
						// System.out.println("OUT v.L_out[(hu >>> 5) %
						// Consts.K]:" + v.id + "=>" + v.L_out[(hu >>> 5) %
						// Consts.K]);

					} else {
						for (int j = 0; j < Consts.K; j++) {
							v.L_out[j] |= w.L_out[j];
						}
					}
				}

				int hu = h_out();
				v.L_out[(hu >>> 5) % Consts.K] |= 1 << (hu & 31);
				// System.out.println("OUT v.L_out[(hu >>> 5) % Consts.K]:" +
				// v.id + "=>" + v.L_out[(hu >>> 5) % Consts.K]);

				stack.pop();
				v.L_interval.mEnd = ++cur;// cur;
			}

		}

	}

	// iterative dfs
	private void dfs_in_iter(GraphNode u) {

		Stack<GraphNode> stack = new Stack<GraphNode>();
		stack.push(u);
		// previsit_in(u);
		if (u.N_I_SZ > 0) {
			// u.N_I_it = u.N_I.iterator();
			for (int i = 0; i < Consts.K; i++) {
				u.L_in[i] = 0;
			}

		}

		u.vis = Flags.vis_cur;
		// System.out.println("IN visiting " + u.id);
		while (!stack.isEmpty()) {
			GraphNode v = stack.peek();

			if (v.N_I_SZ == 0) {

				((GraphNode) v).h_in = (h_in() % (Consts.K * 32));
				stack.pop();

			} else {

				// dfs visting v's parents
				// if (v.N_I_it.hasNext()) {
				// GraphNode w = nodes.get(v.N_I_it.next());
				if (in_it[v.id].hasNext()) {
					GraphNode w = nodes[in_it[v.id].next()];
					if (w.vis != Flags.vis_cur) {
						// dfs_in(w);
						w.vis = Flags.vis_cur;
						stack.push(w);
						// previsit_in(w);

						if (w.N_I_SZ > 0) {
							// w.N_I_it = w.N_I.iterator();
							for (int i = 0; i < Consts.K; i++) {
								w.L_in[i] = 0;
							}

						}

						// System.out.println("IN visiting " + w.id);

					}
					continue;
				}

				for (int i = 0; i < v.N_I_SZ; i++) {
					GraphNode w = nodes[v.N_I.get(i)];
					if (w.N_I_SZ == 0) {
						int hu = ((GraphNode) w).h_in;
						v.L_in[(hu >>> 5) % Consts.K] |= 1 << (hu & 31);
						// System.out.println("IN v.L_in[(hu >>> 5) %
						// Consts.K]:" + v.id + "=>" + v.L_in[(hu >>> 5) %
						// Consts.K]);
					} else {
						for (int j = 0; j < Consts.K; j++) {
							v.L_in[j] |= w.L_in[j];
						}
					}

				}

				int hu = h_in();
				v.L_in[(hu >>> 5) % Consts.K] |= 1 << (hu & 31);
				// System.out.println("IN v.L_in[(hu >>> 5) % Consts.K]:" + v.id
				// + "=>" + v.L_in[(hu >>> 5) % Consts.K]);
				stack.pop();
			}

		}

	}

	public void resetVisFlag() {

		for (GraphNode n : nodes)

			n.vis = 0;
	}

	private int h_in() {
		r_i = rand();
		if (c_i >= (int) nodes.length / Consts.D) {
			c_i = 0;
			r_i = rand();
		}
		c_i++;
		return r_i;
	}

	private int h_out() {
		r_o = rand();
		if (c_o >= (int) nodes.length / Consts.D) {
			c_o = 0;
			r_o = rand();
		}
		c_o++;
		return r_o;
	}

	private int rand() {

		int min = 0, max = Integer.MAX_VALUE;
		int r = ThreadLocalRandom.current().nextInt(min, max);
		return r;
	}

	private void init() {

		nodes = mG.getNodes();
	
		
		in_it = (Iterator<Integer>[]) new Iterator[mG.V()];
		out_it = (Iterator<Integer>[]) new Iterator[mG.V()];
		for (int v = 0; v < mG.V(); v++) {
			GraphNode node = nodes[v];
			if (node.N_I != null)
				in_it[v] = node.N_I.iterator();
			if (node.N_O != null)
				out_it[v] = node.N_O.iterator();
		}
		mInvLsts = new ArrayList<ArrayList<GraphNode>>();
		for (int i = 0; i < mG.getNumLabels(); i++) {
			mInvLsts.add(new ArrayList<GraphNode>());
		}

		lbCounters = new int[mG.getNumLabels()];
	}

	public static void main(String[] args) {

		DigraphLoader loader = new DigraphLoader("E:\\experiments\\datasets\\graphs\\data\\citeseerx.gra");
		// ("E:\\experiments\\datasets\\data\\xm5e.lg");
		// loader.loadVE();
		loader.loadGRA();
		Digraph g = loader.getGraph();
		BFLIndexBuilder bfl = new BFLIndexBuilder(g);
		bfl.run();
	}

}
