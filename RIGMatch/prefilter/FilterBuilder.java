package prefilter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Stack;

import org.roaringbitmap.RoaringBitmap;

import dao.DigraphLoader;
import dao.MatArray;
import global.Consts;
import global.Consts.AxisType;
import global.Flags;
import graph.Digraph;
import graph.GraphNode;
import helper.TimeTracker;
import query.graph.QNode;
import query.graph.Query;
import query.graph.QueryParser;

/*
 * assume that all the query nodes have distinct labels
 * 
 */

public class FilterBuilder {

	private Digraph mG;
	private Query mQ;
	private ArrayList<ArrayList<GraphNode>> mInvLsts_reduced;

	private GraphNode[] mGNodes;
	private QNode[] mQNodes;
	private BitSet[] mQBits_dn, mQBits_up;
	private boolean[][] mSat_dn, mSat_up;
	private BitSet[] mGBit_dn, mGBit_up;
	private Iterator<Integer>[] in_it, out_it;
	private ArrayList<Query> queries;

	private double bldTime = 0.0;
	private double totNodes_after = 0.0;

	public FilterBuilder(Digraph g, Query q) {
		mG = g;
		mQ = q;
	}

	public FilterBuilder(String dataFN, String queryFN) {

		String queryFileN, dataFileN;
		HashMap<String, Integer> l2iMap;

		queryFileN = Consts.INDIR + queryFN;
		dataFileN = Consts.INDIR + dataFN;
		DigraphLoader loader = new DigraphLoader(dataFileN);

		System.out.println("loading data graph ...");
		// load data graph
		loader.loadVE();
		mG = loader.getGraph();
		l2iMap = loader.getL2IMap();
		System.out.println("done. ...");

		System.out.println("loading queries ...");
		// load queries
		QueryParser queryParser = new QueryParser(queryFileN, l2iMap);
		Query query = null;
		queries = new ArrayList<Query>();
		while ((query = queryParser.readNextQuery()) != null) {
			queries.add(query);
		}

		System.out.println("done. ...");
	}

	public ArrayList<ArrayList<GraphNode>> getInvLsts() {

		sortInvLsts();
		return mInvLsts_reduced;
	}

	public ArrayList<RoaringBitmap> getBitsByIDArr() {

		ArrayList<RoaringBitmap> bitsByIDArr = new ArrayList<RoaringBitmap>(mQ.V);

		for (QNode q : mQNodes) {
			RoaringBitmap bits = new RoaringBitmap();
			ArrayList<GraphNode> invLst = mInvLsts_reduced.get(q.id);
			for (GraphNode n : invLst) {
				bits.add(n.id);
			}
			bitsByIDArr.add(bits);
		}
		return bitsByIDArr;

	}

	public RoaringBitmap[] getCandBitsArr() {

		RoaringBitmap[] bitsByIDArr = new RoaringBitmap[mQ.V];

		for (int i = 0; i < mQ.V; i++) {
			QNode q = mQNodes[i];
			RoaringBitmap bits = new RoaringBitmap();
			ArrayList<GraphNode> invLst = mInvLsts_reduced.get(q.id);
			for (GraphNode n : invLst) {
				bits.add(n.id);
			}
			bitsByIDArr[q.id] = bits;
		}
		return bitsByIDArr;

	}

	public ArrayList<ArrayList<GraphNode>> getInvLstsByID() {

		sortInvLstsByID();
		return mInvLsts_reduced;
	}
	
	

	public void run() {

		for (int i = 0; i < queries.size(); i++) {

			mQ = queries.get(i);
			System.out.println("Query:" + mQ);
			oneRun();
			printInvLsts();
		}

	}

	public double getBuildTime() {

		return bldTime;
	}

	public double getTotNodes() {

		for (int i = 0; i < mQNodes.length; i++) {
			QNode q = mQNodes[i];
			ArrayList<GraphNode> invLst = mInvLsts_reduced.get(q.id);
			totNodes_after += invLst.size();
		}

		return totNodes_after;
	}

	public void oneRun() {

		init();

		System.out.println("generating QBits...");
		genQBits();

		TimeTracker tt = new TimeTracker();
		tt.Start();
		Flags.vis_cur++;

		System.out.println("generating downward gBits...");
		for (GraphNode node : mG.getSources()) {

			dfs_out(node);
		}

		Flags.vis_cur++;

		System.out.println("generating upward gBits...");

		for (GraphNode node : mG.getSinks()) {

			dfs_in(node);

		}

		bldTime = tt.Stop() / 1000;
		Flags.mt.run();
		System.out.println("Time for building prefilter index:" + bldTime + "sec.");

		printSize();
		// printInvLsts();
	}

	public ArrayList<MatArray> getCandLists() {

		int size = mQ.V;
		ArrayList<MatArray> candLists = new ArrayList<MatArray>(size);
		;
		QNode[] qnodes = mQ.nodes;
		sortInvLsts();
		for (int i = 0; i < size; i++) {

			QNode q = qnodes[i];
			// mInvLsts_reduced indexed by query id
			ArrayList<GraphNode> invLst = mInvLsts_reduced.get(q.id);
			MatArray mlist = new MatArray();
			mlist.addList(invLst);
			candLists.add(q.id, mlist);
		}

		return candLists;

	}
	
    public void clear(){
    	
    	
	}

	private void init() {

		mGNodes = mG.getNodes();
		mQNodes = mQ.getNodes();
		mGBit_dn = new BitSet[mG.V()];
		mGBit_up = new BitSet[mG.V()];

		in_it = (Iterator<Integer>[]) new Iterator[mG.V()];
		out_it = (Iterator<Integer>[]) new Iterator[mG.V()];

		int qlen = mQNodes.length;
		mSat_dn = new boolean[mG.V()][qlen];
		mSat_up = new boolean[mG.V()][qlen];
		for (int v = 0; v < mG.V(); v++) {
			GraphNode node = mGNodes[v];
			if (node.N_I != null)
				in_it[v] = node.N_I.iterator();
			if (node.N_O != null)
				out_it[v] = node.N_O.iterator();
			mGBit_dn[v] = new BitSet(qlen);
			mGBit_up[v] = new BitSet(qlen);
			for (int q = 0; q < qlen; q++) {
				mSat_dn[v][q] = false;
				mSat_up[v][q] = false;
			}
		}

		mQBits_dn = new BitSet[qlen];
		mQBits_up = new BitSet[qlen];

		for (int i = 0; i < qlen; i++) {
			mQBits_dn[i] = new BitSet(qlen);
			mQBits_up[i] = new BitSet(qlen);
		}

		mInvLsts_reduced = new ArrayList<ArrayList<GraphNode>>(qlen);
		for (QNode q : mQNodes) {
			mInvLsts_reduced.add(q.id, new ArrayList<GraphNode>());
		}

	}

	private void genQBits() {

		int qlen = mQNodes.length;
		boolean[] flags = new boolean[qlen];
		ArrayList<QNode> sources = mQ.getSources();

		for (QNode source : sources) {

			genQBits_dn(source, flags);
		}

		Arrays.fill(flags, false);

		ArrayList<QNode> sinks = mQ.getSinks();
		for (QNode sink : sinks) {

			genQBits_up(sink, flags);
		}
	}

	private void genQBits_up(QNode q, boolean[] flags) {

		int qid = q.id;
		BitSet QBits = mQBits_up[qid];

		ArrayList<QNode> parents = mQ.getParents(qid);
		for (QNode parent : parents) {
			if (!flags[parent.id])
				genQBits_up(parent, flags);
			BitSet QBits_p = mQBits_up[parent.id];
			QBits.or(QBits_p);
		}
		flags[qid] = true;
		QBits.set(qid);

		// System.out.println("UP" + " qid: " + qid + " bits: " + QBits);
	}

	private void genQBits_dn(QNode q, boolean[] flags) {
		int qid = q.id;
		BitSet QBits = mQBits_dn[qid];
		ArrayList<QNode> children = mQ.getChildren(qid);

		for (QNode child : children) {
			if (!flags[child.id])
				genQBits_dn(child, flags);
			BitSet QBits_c = mQBits_dn[child.id];
			QBits.or(QBits_c);
		}
		flags[qid] = true;
		QBits.set(qid);
		// System.out.println("DN" + " qid: " + qid + " bits: " + QBits);
	}

	private void dfs_in(GraphNode u) {

		Stack<GraphNode> stack = new Stack<GraphNode>();
		stack.push(u);
		u.vis = Flags.vis_cur;
		while (!stack.isEmpty()) {
			GraphNode v = stack.peek();
			BitSet gBits_v = mGBit_up[v.id];
			if (v.N_I_SZ == 0) {
				// stack.pop();
			} else {

				if (in_it[v.id].hasNext()) {
					GraphNode w = mGNodes[in_it[v.id].next()];
					if (w.vis != Flags.vis_cur) {
						w.vis = Flags.vis_cur;
						stack.push(w);
					}
					continue;
				}

				for (int i = 0; i < v.N_I_SZ; i++) {
					GraphNode w = mGNodes[v.N_I.get(i)];
					BitSet gBits_w = mGBit_up[w.id];
					gBits_v.or(gBits_w);

				}

			}

			// must traverse the query nodes in the bottom-up way, 2018.11.11
			for (int i = mQNodes.length - 1; i >= 0; i--) {
				QNode q_v = mQNodes[i];
				if (q_v.lb != v.lb)
					continue;
				gBits_v.set(q_v.id);
				BitSet qBits_v = mQBits_up[q_v.id];
				if (qBits_v.equals(bitAND(gBits_v, qBits_v))) {
					// must first satisfy the downward constraint,can the upward
					// constraint be checked
					if (mSat_dn[v.id][q_v.id]) {
						// mSat_up[v.id][q_v.id] = true;
						mInvLsts_reduced.get(q_v.id).add(v);

					}
					
				} else {
					// otherwise clear the bit, 2018.11.10
					gBits_v.clear(q_v.id);
				}
			}

			// System.out.println("UP" + " vid: " + v.id + " bits: "+ gBits_v);
			stack.pop();

		}
	}

	private void dfs_out(GraphNode u) {

		Stack<GraphNode> stack = new Stack<GraphNode>();
		stack.push(u);
		u.vis = Flags.vis_cur;

		while (!stack.isEmpty()) {
			GraphNode v = stack.peek();
			BitSet gBits_v = mGBit_dn[v.id];
			// if(match && bitAND(BitSet gBits, BitSet qBits))
    		// v is a leaf node
			if (v.N_O_SZ == 0) {

			} else {
				if (out_it[v.id].hasNext()) {
					GraphNode w = mGNodes[out_it[v.id].next()];
					if (w.vis != Flags.vis_cur) {
						w.vis = Flags.vis_cur;
						stack.push(w);

					}
					continue;

				}

				for (int i = 0; i < v.N_O_SZ; i++) {
					GraphNode w = mGNodes[v.N_O.get(i)];
					BitSet gBits_w = mGBit_dn[w.id];
					gBits_v.or(gBits_w);

				}

			}

			for (QNode q_v : mQNodes) {
				if (q_v.lb != v.lb)
					continue;
				gBits_v.set(q_v.id);
				BitSet qBits_v = mQBits_dn[q_v.id];
				if (qBits_v.equals(bitAND(gBits_v, qBits_v))) {
					mSat_dn[v.id][q_v.id] = true;
				} else {
					// otherwise clear the bit, 2018.11.10
					gBits_v.clear(q_v.id);
				}

			}

			// System.out.println("UP" + " vid: " + v.id + " bits: "+ gBits_v);
			stack.pop();
		}

	}

	private BitSet bitAND(BitSet gBits, BitSet qBits) {

		BitSet temp = (BitSet) gBits.clone();
		temp.or(gBits);

		temp.and(qBits);
		return temp;
	}

	private void sortInvLsts() {

		for (int i = 0; i < mInvLsts_reduced.size(); i++) {

			ArrayList<GraphNode> elist = mInvLsts_reduced.get(i);
			Collections.sort(elist);

		}
	}

	private void sortInvLstsByID2() {

		for (int i = 0; i < mQNodes.length; i++) {
			QNode q = mQNodes[i];
			ArrayList<GraphNode> invLst = mInvLsts_reduced.get(q.id);
			if (!q.isSource() && q.E_I.get(0).axis == AxisType.child) {

				Collections.sort(invLst, GraphNode.NodeIDComparator);

			} else {

				Collections.sort(invLst); // sort by start value
			}

		}

	}

	private void sortInvLstsByID() {

		for (int i = 0; i < mQNodes.length; i++) {
			QNode q = mQNodes[i];
			ArrayList<GraphNode> invLst = mInvLsts_reduced.get(q.id);
			Collections.sort(invLst, GraphNode.NodeIDComparator);

		
		}

	}
	
	private void printInvLsts() {

		for (int i = 0; i < mInvLsts_reduced.size(); i++) {
			ArrayList<GraphNode> lst = mInvLsts_reduced.get(i);

			System.out.println("Inverted list for lid " + i + ":");
			for (GraphNode n : lst)
				System.out.println(n);

		}

	}

	private void printSize() {

		for (int i = 0; i < mInvLsts_reduced.size(); i++) {

			ArrayList<GraphNode> elist = mInvLsts_reduced.get(i);
			System.out.println(i + ":" + elist.size());
		}
	}

	private BitSet bitComplementAND(BitSet qBits) {

		BitSet temp = (BitSet) qBits.clone();
		temp.flip(0, qBits.size());
		return temp;
	}

	public static void main(String[] args) {
		String dataFileN = "gtest1.txt", queryFileN = "qtest1.txt"; // the query
																	// file
		FilterBuilder fb = new FilterBuilder(dataFileN, queryFileN);
		fb.run();

	}

}
