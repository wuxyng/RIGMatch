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
import global.Flags;
import global.Consts.AxisType;
import global.Consts.DirType;
import graph.GraphNode;
import query.graph.QEdge;
import query.graph.QNode;
import query.graph.Query;
import query.graph.QueryHandler;

public class DagSimMapFilter {

	Query mQuery;
	BFLIndex mBFL;
	ArrayList<Integer> nodesTopoList;
	int passNum = 0;
	GraphNode[] mGraNodes;
	ArrayList<ArrayList<GraphNode>> mInvLstsByID;
	ArrayList<RoaringBitmap> mBitsByIDArr;
	RoaringBitmap[] mCandBitsArr;
	AdjHashMap[][] mFwdAdjMapList, mBwdAdjMapList;
	ArrayList<MatArray> mCandLists;
	int bupc = 0, tdwc = 0;
	boolean invLstByQuery = false;
	int[][] posList;

	public DagSimMapFilter(Query query, GraphNode[] graNodes, ArrayList<ArrayList<GraphNode>> invLstsByID,
			ArrayList<RoaringBitmap> bitsByIDArr, BFLIndex bfl) {

		mQuery = query;
		mBFL = bfl;
		mInvLstsByID = invLstsByID;
		mGraNodes = graNodes;
		mBitsByIDArr = bitsByIDArr;
		init();

	}

	public DagSimMapFilter(Query query, GraphNode[] graNodes, ArrayList<ArrayList<GraphNode>> invLstsByID,
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
			
			System.out.println("Now in pass " + passNum);
			if (Flags.PRUNELIMIT && passNum > Consts.PruneLimit)
				break;
			hasChange = pruneTDW(changed);

			if (hasChange) {
				hasChange = pruneBUP(changed);

			}

		} while (hasChange);

		System.out.println("Total passes: " + passNum);

		// System.out.println("Total bup= " + bupc + " tdw=" + tdwc);
	}

	public void pruneTree() {
		boolean[] changed = new boolean[mQuery.V];
		passNum = 0;
		Arrays.fill(changed, true);

		pruneBUP(changed);
		pruneTDW(changed);

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

	private boolean pruneBUP(boolean[] changed) {

		boolean hasChange = false;

		for (int i = mQuery.V - 1; i >= 0; i--) {
			int qid = nodesTopoList.get(i);
			boolean result = pruneOneStepBUP(qid, changed);
			hasChange = hasChange || result;
		}
		passNum++;
		return hasChange;
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

		if (hasChange)
			changed[qid] = true;
		else if(!changed[qid])
			changed[qid] = false;
		
		return hasChange;

	}

	private void pruneOneStepBUP_c(QNode parent,ArrayList<QNode> qnodes_c, RoaringBitmap candBits, boolean[] changed) {

		// RoaringBitmap rmvBits = candBits.clone();

		for (QNode child : qnodes_c) {
			//if (passNum > 1 && !changed[parent.id] && !changed[child.id]) {
			if (!changed[child.id]) {	
				// System.out.println("Yes pruneOneStepBUP!");
				
				continue;
			}
			RoaringBitmap union = unionBackAdj(child);
			candBits.and(union);
		}

		// rmvBits.xor(candBits);
		// ArrayList<GraphNode> rmvList = bits2list(rmvBits);
		// candList.removeAll(rmvList);
		// clearAdjMap(rmvList, parent, DirType.FWD);

	}

	private RoaringBitmap unionBackAdj(QNode child) {
		
		int[] buffer = new int[256];
		RoaringBitmap candBits = mCandBitsArr[child.id];
		
		RoaringBatchIterator it = candBits.getBatchIterator();
		//RoaringBitmap union = new RoaringBitmap();
		List<RoaringBitmap> orMaps_g = new ArrayList<>();
		while (it.hasNext()) {
			
			int batch = it.nextBatch(buffer);
			List<RoaringBitmap> orMaps= new ArrayList<>();
			for(int i = 0; i<batch; ++i) {
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
		//RoaringBitmap union = new RoaringBitmap();
		List<RoaringBitmap> orMaps_g = new ArrayList<>();
		while (it.hasNext()) {
			
			int batch = it.nextBatch(buffer);
			List<RoaringBitmap> orMaps= new ArrayList<>();
			for(int i = 0; i<batch; ++i) {
				GraphNode gn = this.mGraNodes[buffer[i]];
				orMaps.add(gn.adj_bits_id_o);
			}
			orMaps_g.add(FastAggregation.or(orMaps.iterator()));
		}
		
		RoaringBitmap union = FastAggregation.or(orMaps_g.iterator());

		return union;
	
	}

	private void clearAdjMap(ArrayList<GraphNode> srcList, QNode srcNode, DirType dir) {
		ArrayList<Integer> tarList = null;
		AdjHashMap[] adjMapList = null;
		if (dir == DirType.FWD) {

			tarList = srcNode.N_O;
			adjMapList = mFwdAdjMapList[srcNode.id];
		} else {

			tarList = srcNode.N_I;
			adjMapList = mBwdAdjMapList[srcNode.id];
		}

		if (tarList == null)
			return;

		for (GraphNode n : srcList) {
			AdjHashMap adjMap = adjMapList[posList[srcNode.id][n.id]];
			for (int i : tarList) {
				Integer val = adjMap.getValue(i);

				if (val == null)
					continue;

				
					GraphNode cn = this.mGraNodes[val];
					AdjHashMap[] adjMapList_c = null;
					if (dir == DirType.BWD) {
						adjMapList_c = mFwdAdjMapList[i];
					} else
						adjMapList_c = mBwdAdjMapList[i];
					AdjHashMap adjMap_c = adjMapList_c[posList[i][cn.id]];
					adjMap_c.clear(srcNode.id);
				

				adjMap.clear(i);
			}

		}

	}

	private void pruneOneStepBUP_d(QNode parent, ArrayList<QNode> qnodes_d, ArrayList<GraphNode> candList_p,
			RoaringBitmap candBits_p, boolean[] changed) {

		ArrayList<GraphNode> rmvList = new ArrayList<GraphNode>();
		RoaringBitmap rmvBits = new RoaringBitmap();

		AdjHashMap[] adjMapList_p = mFwdAdjMapList[parent.id];

		for (GraphNode gn : candList_p) {
			AdjHashMap adjmap_p = adjMapList_p[posList[parent.id][gn.id]];
			for (QNode child : qnodes_d) {
				boolean found = false;
				//if (passNum > 1 && !changed[parent.id] && !changed[child.id]) {
				if (!changed[child.id]) {	
					// System.out.println("Yes pruneOneStepBUP!");
				
					continue;
				}
	
				RoaringBitmap candBits_c = mCandBitsArr[child.id];
				Integer val = adjmap_p.getValue(child.id);

				if (val != null) {
					// bupc++;
					if (candBits_c.contains(val))
						continue;
				}

				AdjHashMap[] adjMapList_c = mBwdAdjMapList[child.id];
				MatArray mli = mCandLists.get(child.id);
				for (GraphNode ni : mli.elist()) {

					if (gn.id == ni.id)
						continue;
					AdjHashMap adjmap_c = adjMapList_c[posList[child.id][ni.id]];
					if (mBFL.reach(gn, ni) == 1) {
						found = true;
						adjmap_p.addValue(child.id, ni.id);
						adjmap_c.addValue(parent.id, gn.id);
						break;
					}

				}

				if (!found) {
					rmvList.add(gn);
					rmvBits.add(gn.id);
					break;
				}

			}

		}

		candBits_p.xor(rmvBits);
		candList_p.removeAll(rmvList);
		// clearAdjMap(rmvList, parent, DirType.BWD);
	}

	private boolean pruneTDW(boolean[] changed) {

		boolean hasChange = false;

		for (int qid : nodesTopoList) {
			boolean result = pruneOneStepTDW(qid, changed);
			hasChange = hasChange || result;
		}
		passNum++;
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
		ArrayList<GraphNode> elist = mli.elist();
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

		//RoaringBitmap rmvBits = candBits.clone();

		for (QNode parent : qnodes_c) {
			if (!changed[parent.id]) {	
				// System.out.println("Yes pruneOneStepBUP!");
				// System.out.println("Yes pruneOneStepBUP!");
				
				continue;
			}
			RoaringBitmap union = unionFwdAdj(parent);
			candBits.and(union);
		}

		//rmvBits.xor(candBits);
		//ArrayList<GraphNode> rmvList = bits2list(rmvBits);
		// candList.removeAll(rmvList);
		//clearAdjMap(rmvList, child, DirType.BWD);
	}

	private void pruneOneStepTDW_d(QNode child, ArrayList<QNode> qnodes_d, ArrayList<GraphNode> candList_c,
			RoaringBitmap candBits_c, boolean[] changed) {

		ArrayList<GraphNode> rmvList = new ArrayList<GraphNode>();
		RoaringBitmap rmvBits = new RoaringBitmap();

		AdjHashMap[] adjMapList_c = mBwdAdjMapList[child.id];

		for (GraphNode gn : candList_c) {
			AdjHashMap adjmap_c = adjMapList_c[posList[child.id][gn.id]];
			for (QNode parent : qnodes_d) {
				boolean found = false;
				//if (passNum > 1 && !changed[child.id] && !changed[parent.id]) {
				if (!changed[parent.id]) {		
				// System.out.println("Yes pruneOneStepBUP!");
					
					continue;
				}

				RoaringBitmap candBits_p = mCandBitsArr[parent.id];
				Integer val = adjmap_c.getValue(parent.id);

				if (val != null) {
					if (candBits_p.contains(val))
						continue;
					
				}
				AdjHashMap[] adjMapList_p = mFwdAdjMapList[parent.id];
				MatArray mli = mCandLists.get(parent.id);
				for (GraphNode par : mli.elist()) {

					if (gn.id == par.id)
						continue;
					AdjHashMap adjmap_p = adjMapList_p[posList[parent.id][par.id]];
					if (mBFL.reach(par, gn) == 1) {
						found = true;
						adjmap_p.addValue(child.id, gn.id);
						adjmap_c.addValue(parent.id, par.id);

						break;
					}

				}

				if (!found) {
					rmvList.add(gn);
					rmvBits.add(gn.id);
					break;
				}
			}

		}

		candBits_c.xor(rmvBits);
		candList_c.removeAll(rmvList);
		//clearAdjMap(rmvList, child, DirType.FWD);

	}

	private void init() {

		QueryHandler qh = new QueryHandler();
		// nodesOrder = qh.topologyQue(mQuery);
		nodesTopoList = qh.topologyList(mQuery);

		int size = mQuery.V;

		mFwdAdjMapList = new AdjHashMap[size][];

		mBwdAdjMapList = new AdjHashMap[size][];
		mCandLists = new ArrayList<MatArray>(size);

		mCandBitsArr = new RoaringBitmap[size];

		QNode[] qnodes = mQuery.nodes;
		posList = new int[size][mGraNodes.length];

		for (int i = 0; i < size; i++) {
			QNode q = qnodes[i];
			ArrayList<GraphNode> invLst;
			if (invLstByQuery)
				invLst = mInvLstsByID.get(q.id);
			else
				invLst = mInvLstsByID.get(q.lb);
			AdjHashMap[] adjMap_f = new AdjHashMap[invLst.size()];
			mFwdAdjMapList[q.id] = adjMap_f;
			AdjHashMap[] adjMap_b = new AdjHashMap[invLst.size()];
			mBwdAdjMapList[q.id] = adjMap_b;

			for (int j = 0; j < invLst.size(); j++) {
				GraphNode n = invLst.get(j);
				adjMap_f[j] = new AdjHashMap(mQuery.V);
				adjMap_b[j] = new AdjHashMap(mQuery.V);
				posList[q.id][n.id] = j;
			}

			MatArray mlist = new MatArray();
			mlist.addList(invLst);
			mCandLists.add(q.id, mlist);
			// RoaringBitmap t_bits;
			if (invLstByQuery)
				mCandBitsArr[q.id] = mBitsByIDArr.get(q.id);
			else
				mCandBitsArr[q.id] = mBitsByIDArr.get(q.lb).clone();

			// mCandBitsArr[q.id] = t_bits.clone();
		}

	}

	public static void main(String[] args) {

	}

}
