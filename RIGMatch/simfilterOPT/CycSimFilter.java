package simfilterOPT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.RoaringBatchIterator;
import org.roaringbitmap.RoaringBitmap;
import dao.BFLIndex;
import dao.MatArray;
import global.Consts;
import global.Flags;
import graph.GraphNode;
import query.graph.Cyc2Dag;
import query.graph.QEdge;
import query.graph.QNode;
import query.graph.Query;
import query.graph.QueryHandler;

public class CycSimFilter {

	Query mQuery, mDag;
	BFLIndex mBFL;
	int passNumDag = 0, passNumDet = 0, totPass = 0;
	RoaringBitmap[] mCandBitsArr;
	GraphNode[] mGraNodes;
	ArrayList<Integer> nodesTopoList;
	ArrayList<ArrayList<GraphNode>> mInvLstsByID;
	ArrayList<RoaringBitmap> mBitsByIDArr;
	ArrayList<MatArray> mCandLists;

	HashSet<QEdge> backEdges;

	boolean invLstByQuery = false;

	public CycSimFilter(Query query, GraphNode[] graNodes, ArrayList<ArrayList<GraphNode>> invLstsByID,
			ArrayList<RoaringBitmap> bitsByIDArr) {

		mQuery = query;
		mInvLstsByID = invLstsByID;
		mGraNodes = graNodes;
		mBitsByIDArr = bitsByIDArr;
		init();

	}

	public CycSimFilter(Query query, GraphNode[] graNodes, ArrayList<ArrayList<GraphNode>> invLstsByID,
			ArrayList<RoaringBitmap> bitsByIDArr, boolean invLstByQuery) {

		mQuery = query;
		mInvLstsByID = invLstsByID;
		mGraNodes = graNodes;
		mBitsByIDArr = bitsByIDArr;
		this.invLstByQuery = invLstByQuery;
		init();

	}

	public void prune() {

		boolean[] changed = new boolean[mQuery.V];

		boolean hasChange = false;
		Arrays.fill(changed, true);
		if (mQuery.hasCycle) {
			do {
				if (Flags.PRUNELIMIT && passNumDag > Consts.PruneLimit)
					break;
				pruneDag(changed);
				hasChange = pruneDelta();

			} while (hasChange);
		} else
			pruneDag(changed);
		System.out.println("Total passes: " + totPass);

	}

	public RoaringBitmap[] getCandBits() {

		return mCandBitsArr;
	}

	private void pruneDag(boolean[] changed) {

		passNumDag = 0;

		boolean hasChange = pruneBUP(changed);

		do {

			hasChange = pruneTDW(changed);

			if (hasChange) {
				hasChange = pruneBUP(changed);

			}

		} while (hasChange);

		// System.out.println("Total Dag prune passes: " + passNumDag);
		totPass += passNumDag;

	}

	private boolean pruneDelta() {

		boolean hasChange = false;// = backwardCheck(edges);
		boolean deltaChange = false;
		passNumDet = 0;
		do {
			hasChange = backwardCheck();
			hasChange = hasChange || forwardCheck();
			if (hasChange)
				deltaChange = true;
		} while (hasChange);

		// System.out.println("Total Delta prune passes: " + passNumDet);
		totPass += passNumDet;
		return deltaChange;
	}

	private boolean pruneBUP(boolean[] changed) {

		boolean hasChange = false;

		for (int i = mQuery.V - 1; i >= 0; i--) {
			int qid = nodesTopoList.get(i);
			boolean result = pruneOneStepBUP(qid, changed);
			hasChange = hasChange || result;
		}
		passNumDag++;
		return hasChange;
	}

	private boolean pruneOneStepBUP(int qid, boolean[] changed) {

		QNode[] qnodes = mDag.nodes;
		QNode parent = qnodes[qid];
		if (parent.isSink())
			return false;

		RoaringBitmap candBits = mCandBitsArr[parent.id];
		int card = candBits.getCardinality();

		pruneOneStepBUP_c(parent, candBits, changed);

		boolean hasChange = card > candBits.getCardinality() ? true : false;

		if (hasChange)
			changed[qid] = true;
		else if (!changed[qid])
			changed[qid] = false;

		return hasChange;

	}

	private void pruneOneStepBUP_c(QNode parent, RoaringBitmap candBits, boolean[] changed) {

		for (int child : parent.N_O) {

			if (passNumDag > 1 && !changed[child]) {
				// System.out.println("Yes pruneOneStepBUP!");
				continue;
			}

			RoaringBitmap union = unionBackAdj(child);
			candBits.and(union);
		}

	}

	private boolean pruneTDW(boolean[] changed) {

		boolean hasChange = false;

		for (int qid : nodesTopoList) {
			boolean result = pruneOneStepTDW(qid, changed);
			hasChange = hasChange || result;
		}
		passNumDag++;
		return hasChange;
	}

	private boolean pruneOneStepTDW(int cid, boolean[] changed) {

		QNode[] qnodes = mDag.nodes;

		QNode child = qnodes[cid];

		if (child.isSource())
			return false;

		RoaringBitmap candBits = mCandBitsArr[child.id];
		int card = candBits.getCardinality();

		pruneOneStepTDW_c(child, candBits, changed);

		boolean hasChange = card > candBits.getCardinality() ? true : false;

		if (hasChange)
			changed[cid] = true;
		else if (!changed[cid])
			changed[cid] = false;
		return hasChange;

	}

	private void pruneOneStepTDW_c(QNode child, RoaringBitmap candBits, boolean[] changed) {

		for (int parent : child.N_I) {
			if (passNumDag > 1 && !changed[parent]) {
				// System.out.println("Yes pruneOneStepBUP!");
				continue;
			}

			RoaringBitmap union = unionFwdAdj(parent);
			candBits.and(union);
		}

	}

	private RoaringBitmap unionFwdAdj(int parent) {

		int[] buffer = new int[256];
		RoaringBitmap candBits = mCandBitsArr[parent];

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

	private RoaringBitmap unionBackAdj(int child) {

		int[] buffer = new int[256];
		RoaringBitmap candBits = mCandBitsArr[child];

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

	private void genCandList() {

		QNode[] qnodes = mQuery.nodes;
		mCandLists = new ArrayList<MatArray>(mQuery.V);

		for (int i = 0; i < qnodes.length; i++) {
			QNode q = qnodes[i];
			MatArray mlist = new MatArray();
			RoaringBitmap candBits = mCandBitsArr[q.id];
			ArrayList<GraphNode> list = bits2list(candBits);
			Collections.sort(list);
			mlist.addList(list);
			mCandLists.add(q.id, mlist);
		}

	}

	public ArrayList<MatArray> getCandList() {

		genCandList();
		return mCandLists;
	}

	private ArrayList<GraphNode> bits2list(RoaringBitmap bits) {

		ArrayList<GraphNode> list = new ArrayList<GraphNode>();
		for (int i : bits) {

			list.add(mGraNodes[i]);
		}

		return list;

	}

	//////////////////////////////////////////////////////////////

	private void filterBwd_c(QNode child, RoaringBitmap candBits) {

		RoaringBitmap union = unionBackAdj(child.id);
		candBits.and(union);

	}

	private boolean backwardCheck() {

		boolean hasChange = false;
		for (QEdge e : backEdges) {
			boolean result = backwardCheck(e);
			hasChange = hasChange || result;
		}
		passNumDet++;
		return hasChange;
	}

	private boolean backwardCheck(QEdge e) {

		int from = e.from, to = e.to;
		QNode child = mDag.nodes[to], parent = mDag.nodes[from];
		RoaringBitmap candBits = mCandBitsArr[from];
		int card = candBits.getCardinality();
		filterBwd_c(child, candBits);

		boolean hasChange = card > candBits.getCardinality() ? true : false;

		return hasChange;
	}

	private boolean forwardCheck() {

		boolean hasChange = false;
		for (QEdge e : backEdges) {
			boolean result = forwardCheck(e);
			hasChange = hasChange || result;
		}
		passNumDet++;
		return hasChange;
	}

	private boolean forwardCheck(QEdge e) {

		int from = e.from, to = e.to;
		QNode child = mDag.nodes[to], parent = mDag.nodes[from];
		RoaringBitmap candBits = mCandBitsArr[child.id];
		int card = candBits.getCardinality();

		filterFwd_c(parent, candBits);
		boolean hasChange = card > candBits.getCardinality() ? true : false;

		return hasChange;
	}

	private void filterFwd_c(QNode parent, RoaringBitmap candBits) {

		RoaringBitmap union = unionFwdAdj(parent.id);
		candBits.and(union);

	}

	//////////////////////////////////////////////

	private void init() {

		if (mQuery.hasCycle) {
			Cyc2Dag c2d = new Cyc2Dag(mQuery);
			mDag = c2d.genDag();
			backEdges = c2d.getBackEdges();
		} else
			mDag = mQuery;
		int size = mDag.V;

		QueryHandler qh = new QueryHandler();
		// nodesOrder = qh.topologyQue(mQuery);
		nodesTopoList = qh.topologyList(mDag);
		// System.out.println("topo: " + nodesTopoList);

		mCandBitsArr = new RoaringBitmap[size];
		QNode[] qnodes = mDag.nodes;

		for (int i = 0; i < size; i++) {
			QNode q = qnodes[i];
			ArrayList<GraphNode> invLst;
			if (invLstByQuery)
				invLst = mInvLstsByID.get(q.id);
			else
				invLst = mInvLstsByID.get(q.lb);

			if (invLstByQuery)
				mCandBitsArr[q.id] = mBitsByIDArr.get(q.id);
			else
				mCandBitsArr[q.id] = mBitsByIDArr.get(q.lb).clone();
		}

	}

	public static void main(String[] args) {

	}

}
