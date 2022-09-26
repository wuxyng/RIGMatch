package evaluator;

import java.util.ArrayList;
import java.util.Collections;

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
import graph.GraphNode;
import helper.LimitExceededException;
import helper.QueryEvalStat;
import helper.TimeTracker;
import prefilter.FilterBuilder;
import query.graph.QNode;
import query.graph.Query;
import queryPlan.PlanGenerator;
//import simfilter.DagSimFilter;
import simfilterOPT.DagSimFilter;
//import simfilter.DagSimMapFilter;
import simfilterOPT.DagSimMapFilter;
import tupleEnumerator.TreeTupleEnumCP;

public class TreeHomSim {

	Query mQuery;
	ArrayList<Pool> mPool;
	ArrayList<MatArray> mCandLists;

	BFLIndex mBFL;
	FilterBuilder mFB;
	TimeTracker tt;
	int[] order;
	QNode mRoot;
	GraphNode[] mGraNodes;
	// Iterable<Integer> nodesOrder;

	ArrayList<ArrayList<GraphNode>> mInvLstsByID;
	ArrayList<RoaringBitmap> mBitsByIDArr;

	double numOutTuples;

	TupleHash[] tupleCache;
	boolean simfilter = true;
	boolean enumByBacktracking = false;

	// query is a tree
	public TreeHomSim(Query query, FilterBuilder fb, BFLIndex bfl) {

		mQuery = query;
		mBFL = bfl;
		mGraNodes = mBFL.getGraphNodes();
		mFB = fb;
		order = PlanGenerator.generateTopoQueryPlan(mQuery);
		tt = new TimeTracker();

	}

	public boolean run(QueryEvalStat stat) throws LimitExceededException {

		mFB.oneRun();
		double prunetm = mFB.getBuildTime();
		stat.setPreTime(prunetm);
		System.out.println("Prefilter Prune time:" + prunetm + " sec.");
		ArrayList<MatArray> mCandLists = null;

		if (simfilter) {
			tt.Start();
			mInvLstsByID = mFB.getInvLstsByID();
			mBitsByIDArr = mFB.getBitsByIDArr();
			DagSimMapFilter filter = new DagSimMapFilter(mQuery, mGraNodes, mInvLstsByID, mBitsByIDArr, mBFL, true);
			filter.pruneTree();
			// filter.prune();
			mCandLists = filter.getCandList();
			prunetm += tt.Stop() / 1000;
			stat.setPreTime(prunetm);
			System.out.println("Prune time:" + prunetm + " sec.");
		} else
			mCandLists = mFB.getCandLists();

		tt.Start();
		HybAnsGraphBuilder agBuilder = new HybAnsGraphBuilder(mQuery, mBFL, mCandLists);
		mPool = agBuilder.run();
		double buildtm = tt.Stop() / 1000;
		stat.calAnsGraphSize(mPool);
		stat.setMatchTime(buildtm);
		stat.setTotNodesAfter(calTotCandSolnNodes());
		System.out.println("Answer graph build time:" + buildtm + " sec.");

		tt.Start();
		if (Flags.COUNT) {
			numOutTuples = calTotTreeSolns();
		} else {

			if (enumByBacktracking)
				this.enumTuples_BT();
			else if (simfilter)
				this.enumTuples_TD();

		}

		double enumtm = tt.Stop() / 1000;
		stat.setEnumTime(enumtm);
		System.out.println("Tuple enumeration time:" + enumtm + " sec.");

		stat.setNumSolns(numOutTuples);
		clear();

		return true;
	}

	public void clear() {
		if (mPool != null)
			for (Pool p : mPool)
				p.clear();
	}

	public double getTupleCount() {

		return numOutTuples;
	}

	private ArrayList<MatArray> getCandList() {
		mCandLists = new ArrayList<MatArray>(mQuery.V);
		for (QNode q : mQuery.nodes) {
			// in the order of qid
			ArrayList<GraphNode> list = this.mInvLstsByID.get(q.lb);
			MatArray matArr = new MatArray();
			matArr.addList(list);
			Collections.sort(matArr.elist());
			mCandLists.add(matArr);

		}
		return mCandLists;
	}

	private double calTotInvNodes() {

		double totNodes_before = 0.0;

		for (QNode q : mQuery.nodes) {

			ArrayList<GraphNode> invLst = mInvLstsByID.get(q.lb);
			totNodes_before += invLst.size();
		}

		return totNodes_before;
	}

	private double calTotCandSolnNodes() {

		double totNodes = 0.0;
		for (Pool pool : mPool) {
			ArrayList<PoolEntry> elist = pool.elist();
			totNodes += elist.size();

		}
		return totNodes;
	}

	private double calTotTreeSolns() {

		QNode root = mQuery.getSources().get(0);
		Pool rPool = mPool.get(root.id);
		double totTuples = 0;
		ArrayList<PoolEntry> elist = rPool.elist();
		for (PoolEntry r : elist) {

			totTuples += r.size();

		}
		System.out.println("total number of solution tuples: " + totTuples);
		return totTuples;

	}

	public void printSolutions(ArrayList<PoolEntry> elist) {

		if (elist.isEmpty())
			return;

		for (PoolEntry r : elist) {

			System.out.println(r);

		}

	}

	/**************************
	 * 
	 * Tuple enumeration phase
	 * 
	 *************************/

	// enumeration by back tracking
	private void enumTuples_BT() throws LimitExceededException {

		PoolEntry[] match = new PoolEntry[mQuery.V];
		int[] count = new int[mQuery.V];
		numOutTuples = 0;
		// backtrack(mQuery.V, 0, match, count, stat);
		backtrack(mQuery.V, 0, match);
		System.out.println("Total solution tuples:" + numOutTuples);
	}

	private void backtrack(int max_depth, int depth, PoolEntry[] match) throws LimitExceededException {

		int cur_vertex = order[depth];
		QNode qn = mQuery.getNode(cur_vertex);
		ArrayList<PoolEntry> candList = getCandList(qn, match);
		if (candList.isEmpty()) {

			return;
		}

		for (PoolEntry e : candList) {

			match[qn.id] = e;

			if (depth == max_depth - 1) {

				numOutTuples++;
				// System.out.println(t);
				if (Flags.OUTLIMIT && numOutTuples >= Consts.OutputLimit) {
					throw new LimitExceededException();
				}

			} else

				backtrack(max_depth, depth + 1, match);
			match[qn.id] = null;

		}

	}

	private ArrayList<PoolEntry> getCandList(QNode qn, PoolEntry[] match) {

		int qid = qn.id;

		if (qn.N_I_SZ == 0) {

			return mPool.get(qn.id).elist();
		}
		int pid = mQuery.getNode(qid).N_I.get(0); // tree parent
		PoolEntry pm = match[pid];
		ArrayList<PoolEntry> qmatList = pm.mFwdEntries.get(qid);
		return qmatList;

	}

	/**************************
	 * 
	 * Tuple enumeration phase
	 * 
	 *************************/
	
	
	private void enumTuples_TD() throws LimitExceededException {

		TreeTupleEnumCP ttenum = new TreeTupleEnumCP(mQuery, mPool);
		
		ttenum.enumTuples();

		numOutTuples = ttenum.getTupleCount();
	}


	
	
	public static void main(String[] args) {

	}

}
