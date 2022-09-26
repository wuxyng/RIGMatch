package answerGraphOPT;

import java.util.ArrayList;

import org.roaringbitmap.RoaringBatchIterator;
import org.roaringbitmap.RoaringBitmap;

import dao.BFLIndex;
import dao.MatArray;
import dao.Pool;
import dao.PoolEntry;
import global.Consts.AxisType;
import graph.GraphNode;
import query.graph.QEdge;
import query.graph.QNode;
import query.graph.Query;
import queryPlan.PlanGenerator;

public class HybAnsGraphBuilder {

	Query mQuery;
	ArrayList<Pool> mPool;
	BFLIndex mBFL;
	ArrayList<MatArray> mCandLists;

	public HybAnsGraphBuilder(Query query, BFLIndex bfl, ArrayList<MatArray> candLsts) {

		mQuery = query;
		mBFL = bfl;
		mCandLists = candLsts;
	}

	public ArrayList<Pool> run() {

		RoaringBitmap[] tBitsIdxArr = new RoaringBitmap[mQuery.V];
		initPool(tBitsIdxArr);

		for (QEdge edge : mQuery.edges) {

			linkOneStep(edge, tBitsIdxArr);
		}

		return mPool;

	}

	// for tree/dag
	public ArrayList<Pool> runBUP() {

		RoaringBitmap[] tBitsIdxArr = new RoaringBitmap[mQuery.V];
		initPool(tBitsIdxArr);

		int[] order = PlanGenerator.generateTopoQueryPlan(mQuery);

		for (int i = mQuery.V - 1; i >= 0; i--) {
			QNode q = mQuery.getNode(order[i]);
			ArrayList<QEdge> edges = q.E_I;
			if (edges != null)
				for (QEdge edge : edges) {

					linkOneStep(edge, tBitsIdxArr);
				}
		}

		return mPool;
	}

	public ArrayList<Pool> runTDW() {

		RoaringBitmap[] tBitsIdxArr = new RoaringBitmap[mQuery.V];
		initPool(tBitsIdxArr);

		int[] order = PlanGenerator.generateTopoQueryPlan(mQuery);

		for (int i = 0; i < mQuery.V; i++) {
			QNode q = mQuery.getNode(order[i]);
			ArrayList<QEdge> edges = q.E_I;
			if (edges != null)
				for (QEdge edge : edges) {

					linkOneStep(edge, tBitsIdxArr);
				}
		}

		return mPool;
	}

	private void initPool(RoaringBitmap[] tBitsIdxArr) {

		mPool = new ArrayList<Pool>(mQuery.V);
		QNode[] qnodes = mQuery.nodes;
		for (int i = 0; i < mQuery.V; i++) {
			Pool qAct = new Pool();
			mPool.add(qAct);
			MatArray mli = mCandLists.get(i);
			ArrayList<GraphNode> elist = mli.elist();
			QNode qn = qnodes[i];
			RoaringBitmap t_bits = new RoaringBitmap();
			tBitsIdxArr[i] = t_bits;
			int pos = 0;
			for (GraphNode n : elist) {
				PoolEntry actEntry = new PoolEntry(pos++, qn, n);
				qAct.addEntry(actEntry);
				t_bits.add(actEntry.getValue().L_interval.mStart);
				//t_bits.add(actEntry.getValue().id);
			}

		}

	}

	private void linkOneStep(QEdge edge, RoaringBitmap[] tBitsIdxArr) {
        System.out.println("Edge: " + edge);
		int from = edge.from, to = edge.to;
		AxisType axis = edge.axis;
		Pool pl_f = mPool.get(from), pl_t = mPool.get(to);

		for (PoolEntry e_f : pl_f.elist()) {
			if (axis == AxisType.child)
				linkOneStep(e_f, tBitsIdxArr[to], pl_t.elist());
			else {

				GraphNode n_f = e_f.getValue();
				for (PoolEntry e_t : pl_t.elist()) {

					GraphNode n_t = e_t.getValue();
					if (n_f.id == n_t.id)
						continue;
					if (n_f.L_interval.mEnd < n_t.L_interval.mStart)
						break;
					if (mBFL.reach(n_f, n_t) == 1) {
						
                        //System.out.println("Adding " + n_f.id + "->" + n_t.id);
						e_f.addChild(e_t);
						e_t.addParent(e_f);

					}

				}
			}

		}

	}

	private void linkOneStep(int from, int to, AxisType axis, RoaringBitmap[] tBitsIdxArr) {

		Pool pl_f = mPool.get(from), pl_t = mPool.get(to);

		for (PoolEntry e_f : pl_f.elist()) {
			if (axis == AxisType.child)
				linkOneStep(e_f, tBitsIdxArr[to], pl_t.elist());
			else {

				GraphNode n_f = e_f.getValue();
				for (PoolEntry e_t : pl_t.elist()) {

					GraphNode n_t = e_t.getValue();
					if (n_f.id == n_t.id)
						continue;
					if (n_f.L_interval.mEnd < n_t.L_interval.mStart)
						break;
					if (mBFL.reach(n_f, n_t) == 1) {

						e_f.addChild(e_t);
						e_t.addParent(e_f);

					}

				}
			}

		}

	}

	private boolean linkOneStep(PoolEntry r, RoaringBitmap t_bits, ArrayList<PoolEntry> list) {

		GraphNode s = r.getValue();

		RoaringBitmap rs_and = RoaringBitmap.and(s.adj_bits_o, t_bits);

		if (rs_and.isEmpty())
			return false;

		RoaringBatchIterator it = rs_and.getBatchIterator();
		int[] buffer = new int[256];

		while (it.hasNext()) {

			int batch = it.nextBatch(buffer);
			for(int i = 0; i<batch; ++i) {
				PoolEntry e = list.get(t_bits.rank(buffer[i]) - 1);
				//System.out.println("Adding " + r.getValue().id + "->" + e.
				//		getValue().id);
				r.addChild(e);
				e.addParent(r);
				
			}

		}

		return true;

	}

	public static void main(String[] args) {

	}

}
