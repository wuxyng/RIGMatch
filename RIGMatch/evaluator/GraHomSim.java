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
import graph.GraphNode;
import helper.LimitExceededException;
import helper.QueryEvalStat;
import helper.TimeTracker;
import query.graph.QNode;
import query.graph.Query;
//import simfilter.DagSimFilter;
import simfilterOPT.DagSimFilter;
//import simfilter.DagSimFilterNaive;
import simfilterOPT.DagSimFilterNaive;
//import simfilter.DagSimMapFilter;
import simfilterOPT.DagSimMapFilter;
//import simfilter.GraSimFilterNaive;
import simfilterOPT.GraSimFilterNaive;
//import simfilter.SimFilterBas;
import simfilterOPT.SimFilterBas;
//import simfilter.SimFilterNav;
import simfilterOPT.SimFilterNav;
import tupleEnumerator.HybTupleEnumCache;
//import tupleEnumerator.HybTupleEnumer;
import tupleEnumeratorOPT.HybTupleEnumer;

public class GraHomSim {

	Query mQuery;
	ArrayList<Pool> mPool;
	ArrayList<MatArray> mCandLists;

	BFLIndex mBFL;
	TimeTracker tt;

	GraphNode[] mGraNodes;
	// Iterable<Integer> nodesOrder;

	ArrayList<ArrayList<GraphNode>> mInvLstsByID;
	ArrayList<RoaringBitmap> mBitsByIDArr;

	double numOutTuples;
	boolean simfilter = true;

	HybTupleEnumer tenum;
	// HybTupleEnumCache tenum;

	// query is a dag

	public GraHomSim(Query query, GraphNode[] graNodes,ArrayList<ArrayList<GraphNode>> invLstsByID, ArrayList<RoaringBitmap> bitsByIDArr,
			BFLIndex bfl) {

		mQuery = query;
		mBFL = bfl;
		//mGraNodes = mBFL.getGraphNodes();
		mGraNodes = graNodes;
		mBitsByIDArr = bitsByIDArr;
		mInvLstsByID = invLstsByID;
		tt = new TimeTracker();

	}
	
	public boolean run(QueryEvalStat stat) throws LimitExceededException {

		stat.setTotNodesBefore(calTotInvNodes());
		if (simfilter) {
			
			//SimFilterNav filter = new SimFilterNav(mQuery, mGraNodes, mInvLstsByID, mBitsByIDArr, mBFL);
			//SimFilterBas filter = new SimFilterBas(mQuery, mGraNodes, mInvLstsByID, mBitsByIDArr, mBFL);
			//GraSimFilterNaive filter = new GraSimFilterNaive(mQuery, mGraNodes, mInvLstsByID, mBitsByIDArr, mBFL);
			//DagSimFilterNaive filter= new DagSimFilterNaive(mQuery, mGraNodes, mInvLstsByID, mBitsByIDArr, mBFL);
			DagSimFilter filter = new DagSimFilter(mQuery, mGraNodes, mInvLstsByID, mBitsByIDArr, mBFL);
			//DagSimMapFilter filter = new DagSimMapFilter(mQuery, mGraNodes, mInvLstsByID, mBitsByIDArr, mBFL);
			tt.Start();
			filter.prune();
	
			mCandLists = filter.getCandList();

			// SimFilter filter = new SimFilter(mQuery, mInvLsts, mBFL);
			// mCandLists = filter.prune();

			double prunetm = tt.Stop() / 1000;
			stat.setPreTime(prunetm);
			System.out.println("Prune time:" + prunetm + " sec.");
		} else
			mCandLists = getCandList();

		tt.Start();
		HybAnsGraphBuilder agBuilder = new HybAnsGraphBuilder(mQuery, mBFL, mCandLists);
		mPool = agBuilder.run();
		double buildtm = tt.Stop() / 1000;
		stat.calAnsGraphSize(mPool);
		stat.setMatchTime(buildtm);
		stat.setTotNodesAfter(calTotCandSolnNodes());
		System.out.println("Answer graph build time:" + buildtm + " sec.");

		tt.Start();
		tenum = new HybTupleEnumer(mQuery, mPool);
		// tenum = new HybTupleEnumCache(mQuery, mPool);
		// if (mQuery.isTree()) {
		// numOutTuples = calTotTreeSolns();
		// } else
		numOutTuples = tenum.enumTuples();

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

		if (tenum != null)
			return tenum.getTupleCount();
		return 0;
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

	public static void main(String[] args) {

	}

}
