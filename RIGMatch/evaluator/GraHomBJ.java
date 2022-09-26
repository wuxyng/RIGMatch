package evaluator;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedList;
import org.roaringbitmap.RoaringBitmap;

import binaryJoin.JoinOP;
import binaryJoin.bas.JoinOptimizer;
import dao.BFLIndex;
import dao.Tuple;
import dao.TupleList;
import global.Flags;
import global.Consts.AxisType;
import graph.GraphNode;
import helper.IntPair;
import helper.LimitExceededException;
import helper.QueryEvalStat;
import helper.TimeTracker;
import prefilter.FilterBuilder;
import query.graph.QEdge;
import query.graph.QNode;
import query.graph.Query;

public class GraHomBJ {

	Query mQuery;
	ArrayList<ArrayList<GraphNode>> mInvLsts, mInvLstsByID;
	BFLIndex mBFL;
	//GraphNode[] nodes;
	ArrayList<RoaringBitmap> mBitsIdxArr;

	TupleList[] mEdgeTab;// indexed by query edge id
	TupleList mSolnTab;// final result of the given query

	int[] edgeCard, nodeCard;
	double[] edgeCost;
	IntPair[] sjCard;

	double mTupleCount;
	double plannm;
	TimeTracker tt;
	FilterBuilder mFB;
	JoinOP mJOP;

	public GraHomBJ(Query query, FilterBuilder fb, BFLIndex bfl, ArrayList<ArrayList<GraphNode>> invLstsByID, ArrayList<RoaringBitmap> bitsByIDArr) {

		mQuery = query;
		mBFL = bfl;
		//nodes = mBFL.getGraphNodes();
		mFB = fb;
		mInvLstsByID= invLstsByID;
		mBitsIdxArr = bitsByIDArr;
	}

	public GraHomBJ(Query query, ArrayList<ArrayList<GraphNode>> invLsts, BFLIndex bfl) {

		mQuery = query;
		mBFL = bfl;
		mInvLsts = invLsts;
		//nodes = mBFL.getGraphNodes();

		init();
	}

	// full enum
	public boolean run(QueryEvalStat stat) {

		mFB.oneRun();
		double prunetm = mFB.getBuildTime();
		double totNodes_after = mFB.getTotNodes();
		mInvLsts = mFB.getInvLsts();
		stat.setPreTime(prunetm);
		stat.setTotNodesAfter(totNodes_after);
		System.out.println("Prune time:" + prunetm + " sec.");

		init();

		tt.Start();
		evaluateEdges();
		double edgejointm = tt.Stop() / 1000;
		stat.setMatchTime(edgejointm);
		System.out.println("Edge join time:" + edgejointm + " sec.");

		tt.Start();
		JoinOptimizer jopt = new JoinOptimizer(mQuery, edgeCost, edgeCard, nodeCard);
		QEdge[] order = jopt.orderJoins(true);
		double plantm = tt.Stop() / 1000;
		double plannm = jopt.getNumPlans();
		stat.setPlanTime(plantm);
		stat.setNumPlans(plannm);
		System.out.println("Join plan time:" + plantm + " sec.");

		tt.Start();
		mSolnTab = evaluateJoins(order);
		double enumtm = tt.Stop() / 1000;
		mTupleCount = mSolnTab.cardinality();
		printSolnTab();
		stat.setNumSolns(mTupleCount);
		stat.setEnumTime(enumtm);
		System.out.println("Tuple enumeration time:" + enumtm + " sec.");

		clear();
		return true;
	}

	// full enum
	public QueryEvalStat run(double flttm) {

		tt.Start();
		evaluateEdges();
		double edgejointm = tt.Stop() / 1000;
		System.out.println("Edge join time:" + edgejointm + " sec.");

		tt.Start();
		JoinOptimizer jopt = new JoinOptimizer(mQuery, edgeCost, edgeCard, nodeCard);
		QEdge[] order = jopt.orderJoins(true);
		double plantm = tt.Stop() / 1000;
		plannm = jopt.getNumPlans();
		System.out.println("Join plan time:" + plantm + " sec.");

		tt.Start();
		mSolnTab = evaluateJoins(order);
		mTupleCount = mSolnTab.cardinality();
		printSolnTab();
		double enumtm = tt.Stop() / 1000;
		System.out.println("Tuple enumeration time:" + enumtm + " sec.");

		QueryEvalStat stat = new QueryEvalStat(flttm, plantm, edgejointm, enumtm, mTupleCount, plannm);

		clear();
		return stat;
	}

	public boolean execute(QueryEvalStat stat) throws LimitExceededException {

		mFB.oneRun();
		double prunetm = mFB.getBuildTime();
		double totNodes_after = mFB.getTotNodes();
		mInvLsts = mFB.getInvLsts();
		stat.setPreTime(prunetm);
		stat.setTotNodesAfter(totNodes_after);
		System.out.println("Prune time:" + prunetm + " sec.");

		init();

		tt.Start();
		evaluateEdges();
		double edgejointm = tt.Stop() / 1000;
		stat.setMatchTime(edgejointm);
		System.out.println("Edge join time:" + edgejointm + " sec.");

		tt.Start();
		JoinOptimizer jopt = new JoinOptimizer(mQuery, edgeCost, edgeCard, nodeCard);
		QEdge[] order = jopt.orderJoins(true);
		double plantm = tt.Stop() / 1000;
		double plannm = jopt.getNumPlans();
		stat.setPlanTime(plantm);
		stat.setNumPlans(plannm);
		System.out.println("Join plan time:" + plantm + " sec.");

		tt.Start();
		mTupleCount = execute(order, stat);
		double enumtm = tt.Stop() / 1000;
		System.out.println("Total #tuples = " + mTupleCount);

		// printSolnTab();
		stat.setNumSolns(mTupleCount);
		stat.setEnumTime(enumtm);
		System.out.println("Tuple enumeration time:" + enumtm + " sec.");

		clear();
		return true;
	}

	public double getPlanNums() {

		return plannm;
	}

	public double getTupleCount() {

		if (mJOP != null)
			return mJOP.getTupleCount();
		return 0;
	}

	public void clear() {
		if (mEdgeTab != null)
			for (TupleList t : mEdgeTab) {
				if (t != null)
					t.clear();
			}
		if (mSolnTab != null)
			mSolnTab.clear();
	}

	private void printSolnTab() {

		System.out.println("Total #tuples = " + mSolnTab.cardinality());

		LinkedList<Tuple> tlist = mSolnTab.getList();
		/*
		 * for(Tuple t:tlist){
		 * 
		 * System.out.println(t); }
		 */

	}

	private TupleList evaluateJoins(QEdge[] order) {

		TupleList leftTab = mEdgeTab[order[0].eid];
		for (int i = 1; i < order.length; i++) {
			TupleList rightTab = mEdgeTab[order[i].eid];
			leftTab = mJOP.join(leftTab, rightTab);
		}

		return leftTab;

	}

	private long execute(QEdge[] order, QueryEvalStat stat) throws LimitExceededException {

		TupleList leftTab = mEdgeTab[order[0].eid];
		for (int i = 1; i < order.length - 1; i++) {
			TupleList rightTab = mEdgeTab[order[i].eid];
			leftTab = mJOP.join(leftTab, rightTab);
		}

		if (Flags.COUNT)
			mJOP.joinCount(leftTab, mEdgeTab[order[order.length - 1].eid]);
		else
			mJOP.joinFinal(leftTab, mEdgeTab[order[order.length - 1].eid]);

		return mJOP.getNumOutTuples();

	}

	private IntPair computeSemiJoinCardinality(QEdge e, TupleList tlist) {

		BitSet s1 = new BitSet(), s2 = new BitSet();

		for (Tuple t : tlist.getList()) {

			s1.set(t.getValue(e.from));
			s2.set(t.getValue(e.to));
		}

		IntPair pair = new IntPair(s1.cardinality(), s2.cardinality());

		return pair;
	}

	private void evaluateEdges() {

		QEdge[] edges = mQuery.edges;
		QNode[] nodes = mQuery.nodes;
		edgeCard = new int[mQuery.E];
		edgeCost = new double[mQuery.E];
		mEdgeTab = new TupleList[mQuery.E];
		for (QEdge e : edges) {
			// note the id is used here
			edgeCost[e.eid] = nodeCard[e.from] * nodeCard[e.to];

			if (e.axis == AxisType.descendant) {
				// descendant
				// TupleList join(ArrayList<GraphNode> from,
				// ArrayList<GraphNode> to, BFLIndex bfl)
                // nodes in from and to are indexed by interval code
				ArrayList<GraphNode> from = mInvLsts.get(nodes[e.from].id), to = mInvLsts.get(nodes[e.to].id);
				
				mEdgeTab[e.eid] = mJOP.join(mQuery.V, e, from, to, mBFL);
			} else {
				// child
				// join(ArrayList<GraphNode> from, ArrayList<GraphNode> to,
				// RoaringBitmap t_bits)
				ArrayList<GraphNode> from = mInvLstsByID.get(nodes[e.from].lb), to = mInvLstsByID.get(nodes[e.to].lb);
				RoaringBitmap t_bits = mBitsIdxArr.get(nodes[e.to].lb);
				mEdgeTab[e.eid] = mJOP.join(mQuery.V, e, from, to, t_bits);

			}

			edgeCard[e.eid] = mEdgeTab[e.eid].cardinality();

		}

	}

	private void prune() {

		mFB.oneRun();
		double bldTM = mFB.getBuildTime();
		double totNodes_after = mFB.getTotNodes();

	}

	private void init() {
		tt = new TimeTracker();
		int size = mQuery.V;
		nodeCard = new int[size];
		mJOP = new JoinOP();

	}

	public static void main(String[] args) {

	}

}
