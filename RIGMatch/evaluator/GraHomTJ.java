package evaluator;

import java.util.ArrayList;
import java.util.Set;

import org.roaringbitmap.RoaringBitmap;

//import answerGraph.HybAnsGraphBuilder;
import answerGraphOPT.HybAnsGraphBuilder;
import dao.BFLIndex;
import dao.MatArray;
import dao.Pool;
import dao.PoolEntry;
import dao.Tuple;
import dao.TupleHash;
import global.Consts;
import global.Flags;
import global.Consts.AxisType;
import graph.GraphNode;
import helper.CartesianProduct;
import helper.LimitExceededException;
import helper.QueryEvalStat;
import helper.TimeTracker;
import prefilter.FilterBuilder;
import query.graph.Dag2Tree;
import query.graph.QEdge;
import query.graph.QNode;
import query.graph.Query;
import queryPlan.PlanGenerator;
//import simfilter.DagSimMapFilter;
import simfilterOPT.DagSimMapFilter;

public class GraHomTJ {

	Query mQuery, mTree;
	ArrayList<ArrayList<GraphNode>> mInvLsts;
	BFLIndex mBFL;
	TimeTracker tt;
	GraphNode[] nodes;
	FilterBuilder mFB;
	ArrayList<Pool> mPool;
	int[] order;
	Set<QEdge> delta;
	double mTupleCount;
	QNode mRoot;
	ArrayList<ArrayList<GraphNode>> mInvLstsByID;
	ArrayList<RoaringBitmap> mBitsByIDArr;
	TupleHash[] tupleCache;
	boolean prefilter = false;

	public GraHomTJ(Query query, ArrayList<ArrayList<GraphNode>> invLstsByID, ArrayList<RoaringBitmap> bitsByIDArr,
			FilterBuilder fb, BFLIndex bfl) {

		mQuery = query;
		mBFL = bfl;
		nodes = mBFL.getGraphNodes();
		mBitsByIDArr = bitsByIDArr;
		mInvLstsByID = invLstsByID;
		mFB = fb;
		tt = new TimeTracker();
		init();
	}

	public boolean run(QueryEvalStat stat) throws LimitExceededException {
		double prunetm = 0;
		DagSimMapFilter filter = null;
		if (prefilter) {
			mFB.oneRun();
			prunetm = mFB.getBuildTime();
			mInvLstsByID = mFB.getInvLstsByID();
			mBitsByIDArr = mFB.getBitsByIDArr();
			System.out.println("PrePrune time:" + prunetm + " sec.");
			// ArrayList<MatArray> mCandLists = mFB.getCandLists();
		}

		tt.Start();
		if (prefilter)
			filter = new DagSimMapFilter(mQuery, nodes, mInvLstsByID, mBitsByIDArr, mBFL, true);
		else
			filter = new DagSimMapFilter(mQuery, nodes, mInvLstsByID, mBitsByIDArr, mBFL);
		filter.prune();

		ArrayList<MatArray> mCandLists = filter.getCandList();
		prunetm += tt.Stop() / 1000;
		stat.setPreTime(prunetm);
		System.out.println("Prune time:" + prunetm + " sec.");

		tt.Start();
		HybAnsGraphBuilder agBuilder = new HybAnsGraphBuilder(mTree, mBFL, mCandLists);
		mPool = agBuilder.run();

		double buildtm = tt.Stop() / 1000;
		stat.setMatchTime(buildtm);
		stat.calAnsGraphSize(mPool);
		stat.setTotNodesAfter(calTotCandSolnNodes());
		System.out.println("Answer graph build time:" + buildtm + " sec.");
		
		tt.Start();
		this.enumTuples();

		double enumtm = tt.Stop() / 1000;
		stat.setEnumTime(enumtm);
		System.out.println("Tuple enumeration time:" + enumtm + " sec.");

		stat.setNumSolns(mTupleCount);
		clear();

		return true;
	}

	public void clear() {
		if (mPool != null)
			for (Pool p : mPool)
				p.clear();
	}

	public double getTupleCount() {

		return mTupleCount;
	}

	////////////////////////////////////

	/**************************
	 * 
	 * Tuple enumeration phase
	 * 
	 *************************/

	private void enumTuples() throws LimitExceededException {

		PoolEntry[] match = new PoolEntry[mQuery.V];
		int[] count = new int[mQuery.V];
		mTupleCount = 0;
		// backtrack(mQuery.V, 0, match, count, stat);
		backtrack(mQuery.V, 0, match);
		System.out.println("Total solution tuples:" + mTupleCount);
	}

	private void backtrack(int max_depth, int depth, PoolEntry[] match) throws LimitExceededException {

		int cur_vertex = order[depth];
		QNode qn = mTree.getNode(cur_vertex);
		ArrayList<PoolEntry> candList = getCandList(qn, match);
		if (candList.isEmpty()) {

			return;
		}

		for (PoolEntry e : candList) {

			match[qn.id] = e;

			if (depth == max_depth - 1) {

				if (checkTuple(match)) {
					mTupleCount++;
					// System.out.println(t);
					if (Flags.OUTLIMIT && mTupleCount >= Consts.OutputLimit) {
						throw new LimitExceededException();
					}
				}

			} else

				backtrack(max_depth, depth + 1, match);
			match[qn.id] = null;

		}

	}

	private boolean checkTuple(PoolEntry[] match) {

		for (QEdge e : delta) {
			AxisType axis = e.axis;
			GraphNode s = match[e.from].getValue();
			GraphNode t = match[e.to].getValue();
			if (s.id == t.id)
				return false;
			if (axis == AxisType.child) {
				if (!s.searchOUT(t.id))
					return false;

			} else {
				if (mBFL.reach(s, t) == 0) {
					return false;
				}
			}

		}

		return true;
	}

	private ArrayList<PoolEntry> getCandList(QNode qn, PoolEntry[] match) {

		// int num = qn.N_I_SZ;
		int qid = qn.id;

		if (qn.isSource()) {

			return mPool.get(qn.id).elist();
		}
		int pid = mTree.getNode(qid).N_I.get(0); // tree parent
		PoolEntry pm = match[pid];
		ArrayList<PoolEntry> qmatList = pm.mFwdEntries.get(qid);
		return qmatList;

	}

	private void printMatch(PoolEntry[] match) {

		for (PoolEntry v : match) {

			System.out.print(v + " ");
		}

		System.out.println();
	}

	/////////////////////////////////////

	private void init() {
		order = PlanGenerator.generateTopoQueryPlan(mQuery);
		Dag2Tree d2t = new Dag2Tree(mQuery);
		mTree = d2t.genTree();
		delta = d2t.getDeltaEdges();
		GraphNode g = new GraphNode();
		tupleCache = new TupleHash[mQuery.V];

		for (int i = 0; i < mTree.V; i++) {

			tupleCache[i] = new TupleHash();
		}
		tt = new TimeTracker();

	}

	private double calTotCandSolnNodes() {

		double totNodes = 0.0;
		for (Pool pool : mPool) {
			ArrayList<PoolEntry> elist = pool.elist();
			totNodes += elist.size();

		}
		return totNodes;
	}

	public static void main(String[] args) {

	}

}
