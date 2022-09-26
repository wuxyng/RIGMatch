package simfilterOPT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.RoaringBatchIterator;
import org.roaringbitmap.RoaringBitmap;

import dao.BFLIndex;
import dao.MatArray;
import global.Consts;
import global.Consts.AxisType;
import global.Flags;
import graph.GraphNode;
import helper.TimeTracker;
import query.graph.QEdge;
import query.graph.QNode;
import query.graph.Query;
import query.graph.QueryHandler;

public class DagSimFilter {

	Query mQuery;
	ArrayList<MatArray> mCandLists;
	BFLIndex mBFL;
	ArrayList<Integer> nodesTopoList;
	int passNum = 0;
	ArrayList<ArrayList<GraphNode>> mInvLstsByID;
	ArrayList<RoaringBitmap> mBitsByIDArr;
	RoaringBitmap[] mCandBitsArr;
	GraphNode[] mGraNodes;
	boolean invLstByQuery = false;

	public DagSimFilter(Query query, GraphNode[] graNodes, ArrayList<ArrayList<GraphNode>> invLstsByID,
			ArrayList<RoaringBitmap> bitsByIDArr, BFLIndex bfl) {

		mQuery = query;
		mBFL = bfl;
		mInvLstsByID = invLstsByID;
		mGraNodes = graNodes;
		mBitsByIDArr = bitsByIDArr;
		init();
	}

	public DagSimFilter(Query query, GraphNode[] graNodes, ArrayList<ArrayList<GraphNode>> invLstsByID,
			ArrayList<RoaringBitmap> bitsByIDArr, BFLIndex bfl, boolean invLstByQuery) {

		mQuery = query;
		mBFL = bfl;
		mInvLstsByID = invLstsByID;
		mGraNodes = graNodes;
		mBitsByIDArr = bitsByIDArr;
		this.invLstByQuery = invLstByQuery;
		init();

	}

	public void prune() {
		boolean[] changed = new boolean[mQuery.V];
		passNum = 0;
		Arrays.fill(changed, true);

		boolean hasChange = pruneBUP(changed);

		do {
			if (Flags.PRUNELIMIT && passNum > Consts.PruneLimit)
			    break;
			hasChange = pruneTDW(changed);
			if (hasChange) {
				hasChange = pruneBUP(changed);

			}

		} while (hasChange);

		System.out.println("Total passes: " + passNum);

	}

	public ArrayList<MatArray> getCandList() {

		for (int i = 0; i < mQuery.V; i++) {
			QNode q = mQuery.nodes[i];
			ArrayList<GraphNode> list = mCandLists.get(i).elist();
			Collections.sort(list);
			// System.out.println("qid = " + i +" " + " inv= " +
			// this.mInvLstsByID.get(q.lb).size()+ " original bits = " +
			// this.mBitsByIDArr.get(q.lb).getCardinality() + " bits = " +
			// this.mCandBitsArr[i].getCardinality() + " list = " +
			// this.mCandLists.get(i).elist().size());
		}
		return mCandLists;
	}

	public ArrayList<RoaringBitmap> getBitsByIDArr() {

		ArrayList<RoaringBitmap> bitsByIDArr = new ArrayList<RoaringBitmap>(mQuery.V);

		for (int i = 0; i < mQuery.V; i++) {

			System.out.println("bits card=" + mCandBitsArr[i].getCardinality() + ", list len ="
					+ mCandLists.get(i).elist().size());
			bitsByIDArr.add(mCandBitsArr[i]);
		}
		return bitsByIDArr;

	}

	private boolean pruneBUP(boolean[] changed) {

		boolean hasChange = false;
		//System.out.println("Passnum=" + passNum);
		//System.out.println("in pruneBUP");
		// printCard();
		for (int i = mQuery.V - 1; i >= 0; i--) {
			int qid = nodesTopoList.get(i);
			boolean result = pruneOneStepBUP(qid, changed);
			hasChange = hasChange || result;

		}
		passNum++;
		//System.out.println("out pruneBUP");

		// printCard();

		return hasChange;
	}

	private void printCard() {

		for (int i = 0; i < mQuery.V; i++) {

			System.out.println("card(" + i + ")=" + this.mCandBitsArr[i].getCardinality());

		}
	}

	private ArrayList<GraphNode> bits2list(RoaringBitmap bits) {

		ArrayList<GraphNode> list = new ArrayList<GraphNode>();
		for (int i : bits) {

			list.add(mGraNodes[i]);
		}

		return list;

	}

	private boolean pruneOneStepBUP(int qid, boolean[] changed) {

		QNode[] qnodes = mQuery.nodes;
		QNode parent = qnodes[qid];
		if (parent.isSink())
			return false;

		RoaringBitmap candBits = mCandBitsArr[parent.id];
		int card = candBits.getCardinality();
		MatArray mli = mCandLists.get(parent.id);
		ArrayList<QEdge> o_edges = parent.E_O;
		ArrayList<QNode> qnodes_c = new ArrayList<QNode>(o_edges.size()),
				qnodes_d = new ArrayList<QNode>(o_edges.size());

		for (QEdge o_edge : o_edges) {
			int cid = o_edge.to;
			AxisType axis = o_edge.axis;
			QNode child = qnodes[cid];
			if (axis == AxisType.child)
				qnodes_c.add(child);
			else
				qnodes_d.add(child);
		}

		if (qnodes_c.size() > 0) {

			pruneOneStepBUP_c(parent, qnodes_c, candBits, changed);

			mli.setList(bits2list(candBits));
		}

		if (qnodes_d.size() > 0) {

			pruneOneStepBUP_d(parent, qnodes_d, mli.elist(), candBits, changed);

		}

		boolean hasChange = card > candBits.getCardinality() ? true : false;
/*
		if (hasChange)
			changed[qid] = true;
		else
			changed[qid] = false;
*/		
		if (hasChange)
			changed[qid] = true;
		else if(!changed[qid])
			changed[qid] = false;
		
		return hasChange;

	}

	private void pruneOneStepBUP_c(QNode parent, ArrayList<QNode> qnodes_c, RoaringBitmap candBits, boolean[] changed) {

		RoaringBitmap rmvBits = candBits.clone();

		for (QNode child : qnodes_c) {
			//if (passNum > 1 && !changed[parent.id] && !changed[child.id]) {
			if (!changed[child.id]) {	
				// System.out.println("Yes pruneOneStepBUP!");

				continue;
			}
			RoaringBitmap union = unionBackAdj(child);
			candBits.and(union);
		}

		
		rmvBits.xor(candBits);
		//System.out.println("#buppassc:" + passNum + " parent: " + parent.id + " pruned:");
		//System.out.println(rmvBits);
		//System.out.println();
		// ArrayList<GraphNode> rmvList = bits2list(rmvBits);
		// candList.removeAll(rmvList);

	}

	private RoaringBitmap unionBackAdj(QNode child) {

		int[] buffer = new int[256];
		RoaringBitmap candBits = mCandBitsArr[child.id];

		RoaringBatchIterator it = candBits.getBatchIterator();
		// RoaringBitmap union = new RoaringBitmap();
		List<RoaringBitmap> orMaps_g = new ArrayList<>();
		while (it.hasNext()) {

			int batch = it.nextBatch(buffer);
			List<RoaringBitmap> orMaps = new ArrayList<>();
			for (int i = 0; i < batch; ++i) {
				GraphNode gn = this.mGraNodes[buffer[i]];
				orMaps.add(gn.adj_bits_id_i);
			}
			orMaps_g.add(FastAggregation.or(orMaps.iterator()));
		}

		RoaringBitmap union = FastAggregation.or(orMaps_g.iterator());

		return union;

	}

	private RoaringBitmap unionFwdAdj(QNode parent) {

		int[] buffer = new int[256];
		RoaringBitmap candBits = mCandBitsArr[parent.id];

		RoaringBatchIterator it = candBits.getBatchIterator();
		// RoaringBitmap union = new RoaringBitmap();
		List<RoaringBitmap> orMaps_g = new ArrayList<>();
		while (it.hasNext()) {

			int batch = it.nextBatch(buffer);
			List<RoaringBitmap> orMaps = new ArrayList<>();
			for (int i = 0; i < batch; ++i) {
				GraphNode gn = this.mGraNodes[buffer[i]];
				orMaps.add(gn.adj_bits_id_o);
			}
			orMaps_g.add(FastAggregation.or(orMaps.iterator()));
		}

		RoaringBitmap union = FastAggregation.or(orMaps_g.iterator());

		return union;

	}

	private void pruneOneStepBUP_d(QNode parent, ArrayList<QNode> qnodes_d, ArrayList<GraphNode> candList_p,
			RoaringBitmap candBits_p, boolean[] changed) {

		ArrayList<GraphNode> rmvList = new ArrayList<GraphNode>();
		RoaringBitmap rmvBits = new RoaringBitmap();
		for (GraphNode gn : candList_p) {
			for (QNode child : qnodes_d) {
				boolean found = false;
				//if (passNum > 1 && !changed[parent.id] && !changed[child.id]) {
				if (!changed[child.id]) {	
					// System.out.println("Yes pruneOneStepBUP!");

					continue;
				}

				MatArray mli = mCandLists.get(child.id);
				for (GraphNode ni : mli.elist()) {

					if (gn.id == ni.id)
						continue;

					if (mBFL.reach(gn, ni) == 1) {
						//System.out.println("#buppass:" +passNum + "," + "pid:" +parent.id + "," + "gid:" + gn.id + ",cid:" + child.id + "," + "gid:" + ni.id );
						found = true;
						break;
					}

				}

				if (!found) {
					rmvList.add(gn);
					rmvBits.add(gn.id);
					//System.out.println("#buppass:" +passNum + "," + "pid:" +parent.id + "," + "gid:" + gn.id + " PRUNED");
					break;
				}
			}

		}

		candBits_p.xor(rmvBits);
		candList_p.removeAll(rmvList);

	}

	private boolean pruneTDW(boolean[] changed) {

		boolean hasChange = false;
		//System.out.println("Passnum=" + passNum);
		//System.out.println("in pruneTDW");
		//printCard();
		for (int qid : nodesTopoList) {
			boolean result = pruneOneStepTDW(qid, changed);
			hasChange = hasChange || result;
		}
		passNum++;
		//System.out.println("out pruneTDW");
		//printCard();
		return hasChange;
	}

	private boolean pruneOneStepTDW(int cid, boolean[] changed) {

		QNode[] qnodes = mQuery.nodes;

		QNode child = qnodes[cid];

		if (child.isSource())
			return false;

		RoaringBitmap candBits = mCandBitsArr[child.id];
		int card = candBits.getCardinality();
		MatArray mli = mCandLists.get(child.id);
		ArrayList<QEdge> i_edges = child.E_I;
		ArrayList<QNode> qnodes_c = new ArrayList<QNode>(i_edges.size()),
				qnodes_d = new ArrayList<QNode>(i_edges.size());

		for (QEdge i_edge : i_edges) {
			int pid = i_edge.from;
			AxisType axis = i_edge.axis;
			QNode parent = qnodes[pid];
			if (axis == AxisType.child)
				qnodes_c.add(parent);
			else
				qnodes_d.add(parent);
		}

		if (qnodes_c.size() > 0) {

			pruneOneStepTDW_c(child, qnodes_c, candBits, changed);
			mli.setList(bits2list(candBits));
		}

		if (qnodes_d.size() > 0) {

			pruneOneStepTDW_d(child, qnodes_d, mli.elist(), candBits, changed);

		}

		boolean hasChange = card > candBits.getCardinality() ? true : false;
			
		if (hasChange)
			changed[cid] = true;
		else if(!changed[cid])
			changed[cid] = false;
		
		return hasChange;

	}

	private void pruneOneStepTDW_c(QNode child, ArrayList<QNode> qnodes_c, RoaringBitmap candBits, boolean[] changed) {

		 RoaringBitmap rmvBits = candBits.clone();

		for (QNode parent : qnodes_c) {
			//if (passNum > 1 && !changed[child.id] && !changed[parent.id]) {
			if (!changed[parent.id]) {	
				// System.out.println("Yes pruneOneStepBUP!");

				continue;
			}
			RoaringBitmap union = unionFwdAdj(parent);
			candBits.and(union);
		}

		 rmvBits.xor(candBits);
		 //System.out.println("#tdwpassc:" + passNum + " child: " + child.id + " pruned");
		 //System.out.println(rmvBits);
		 //System.out.println();
		// ArrayList<GraphNode> rmvList = bits2list(rmvBits);
		// candList.removeAll(rmvList);

	}

	private void pruneOneStepTDW_d(QNode child, ArrayList<QNode> qnodes_d, ArrayList<GraphNode> candList_c,
			RoaringBitmap candBits_c, boolean[] changed) {

		ArrayList<GraphNode> rmvList = new ArrayList<GraphNode>();
		RoaringBitmap rmvBits = new RoaringBitmap();
		for (GraphNode gn : candList_c) {
			for (QNode parent : qnodes_d) {
				boolean found = false;
				//if (passNum > 1 && !changed[child.id] && !changed[parent.id]) {
				if (!changed[parent.id]) {		
					// System.out.println("Yes pruneOneStepBUP!");

					continue;
				}

				MatArray mli = mCandLists.get(parent.id);
				for (GraphNode par : mli.elist()) {

					if (gn.id == par.id)
						continue;

					if (mBFL.reach(par, gn) == 1) {
						//System.out.println("#tdwpass:" +passNum + "," + "pid:" +parent.id + "," + "gid:" + par.id + ",cid:" + child.id + "," + "gid:" + gn.id );
						found = true;
						break;
					}

				}

				if (!found) {
					rmvList.add(gn);
					rmvBits.add(gn.id);
					//System.out.println("#tdwpass:" +passNum + "," + "cid:" +child.id + "," + "gid:" + gn.id + " PRUNED");
					
					break;
				}
			}

		}

		candBits_c.xor(rmvBits);
		candList_c.removeAll(rmvList);

	}

	private void init() {

		QueryHandler qh = new QueryHandler();
		// nodesOrder = qh.topologyQue(mQuery);
		nodesTopoList = qh.topologyList(mQuery);

		int size = mQuery.V;
		mCandLists = new ArrayList<MatArray>(size);

		mCandBitsArr = new RoaringBitmap[size];

		QNode[] qnodes = mQuery.nodes;
		for (int i = 0; i < qnodes.length; i++) {
			QNode q = qnodes[i];

			ArrayList<GraphNode> invLst;
			if (invLstByQuery)
				invLst = mInvLstsByID.get(q.id);
			else
				invLst = mInvLstsByID.get(q.lb);
			MatArray mlist = new MatArray();
			mlist.addList(invLst);
			mCandLists.add(q.id, mlist);

			if (invLstByQuery)
				mCandBitsArr[q.id] = mBitsByIDArr.get(q.id);
			else
				mCandBitsArr[q.id] = mBitsByIDArr.get(q.lb).clone();
		}

	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
