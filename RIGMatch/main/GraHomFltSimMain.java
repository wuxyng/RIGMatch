package main;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.UncheckedTimeoutException;

import dao.BFLIndex;
import dao.DaoController;
import evaluator.GraHomFltSim;
import global.Consts;
import global.Consts.AxisType;
import global.Consts.status_vals;
import global.Flags;
import graph.Digraph;
import graph.GraphNode;
import helper.LimitExceededException;
import helper.QueryEvalStat;
import helper.QueryEvalStats;
import helper.TimeTracker;
import prefilter.FilterBuilder;
import query.graph.QEdge;
import query.graph.QNode;
import query.graph.Query;
import query.graph.QueryDirectedCycle;
import query.graph.QueryParser;
import query.graph.TransitiveReduction;

public class GraHomFltSimMain {

	ArrayList<Query> queries;
	HashMap<String, Integer> l2iMap;
	String queryFileN, dataFileN, outFileN;
	ArrayList<ArrayList<GraphNode>> invLsts;
	BFLIndex bfl;
	TimeTracker tt;
	QueryEvalStats stats;
	Digraph g;

	public GraHomFltSimMain(String dataFN, String queryFN) {

		queryFileN = Consts.INDIR + queryFN;
		dataFileN = Consts.INDIR + dataFN;
		String suffix = ".csv";
		String fn = queryFN.substring(0, queryFN.lastIndexOf('.'));
		outFileN = Consts.OUTDIR + "sum_" + fn + "dag_IEfltsim" + suffix;
		stats = new QueryEvalStats(dataFileN, queryFileN, "DagEval_IEfltsim");

	}

	public void run() {

		tt = new TimeTracker();

		System.out.println("loading graph ...");
		tt.Start();
		loadData();
		double ltm = tt.Stop() / 1000;
		System.out.println("\nTotal loading and building time: " + ltm + "sec.");

		System.out.println("reading queries ...");
		readQueries();

		System.out.println("\nEvaluating queries from " + queryFileN + " ...");
		tt.Start();
		evaluate();
		System.out.println("\nTotal eval time: " + tt.Stop() / 1000 + "sec.");

		writeStatsToCSV();
		// skip the execution of the timeout tasks;
		System.exit(0);
	}

	private void loadData() {
		DaoController dao = new DaoController(dataFileN, stats);
		dao.loadGraphAndIndex();
		invLsts = dao.invLsts;
		l2iMap = dao.l2iMap;
		bfl = dao.bfl;
		g = dao.G;
	}

	private void readQueries() {

		queries = new ArrayList<Query>();
		QueryParser queryParser = new QueryParser(queryFileN, l2iMap);
		Query query = null;
		int count = 0;

		while ((query = queryParser.readNextQuery()) != null) {
			// System.out.println(query);
			TransitiveReduction tr = new TransitiveReduction(query);
			tr.reduce();
			System.out.println(query);
			checkQueryType(query);
			// if (!query.childOnly && !query.hasCycle) {

			queries.add(query);
			count++;
			// }
			if (query.childOnly) {

				System.out.println("Child only query:" + query.Qid);

			}
		}

		System.out.println("Total valid queries: " + count);
	}

	private void evaluate() {

		TimeTracker tt = new TimeTracker();
		for (int i = 0; i < Flags.REPEATS; i++) {
			for (int Q = 0; Q < queries.size(); Q++) {

				Query query = queries.get(Q);
				System.out.println("\nEvaluating query " + Q + " ...");
				print(query);
				double totNodes_before = getTotNodes(query);
				FilterBuilder fb = new FilterBuilder(g, query);
				java.util.concurrent.ExecutorService executor = Executors.newSingleThreadExecutor();
				SimpleTimeLimiter timeout = new SimpleTimeLimiter(executor);
				GraHomFltSim eva = new GraHomFltSim(query, fb, bfl);

				QueryEvalStat stat = null;
				final QueryEvalStat s = new QueryEvalStat();
				s.totNodesBefore = totNodes_before;
				try {
					tt.Start();
					timeout.callWithTimeout(new Callable<Boolean>() {

						public Boolean call() throws Exception {
							return eva.run(s);
						}
					}, Consts.TimeLimit, TimeUnit.MINUTES, false);

					stat = new QueryEvalStat(s);
					stats.add(i, Q, stat);

				} catch (UncheckedTimeoutException e) {
					eva.clear();
					s.numSolns = eva.getTupleCount();
					stat = new QueryEvalStat(s);
					stat.setStatus(status_vals.timeout);
					stats.add(i, Q, stat);
					System.err.println("Time Out!");

				} catch (OutOfMemoryError e) {
					eva.clear();
					s.numSolns = eva.getTupleCount();
					stat = new QueryEvalStat(s);
					stat.setStatus(status_vals.outOfMemory);
					stats.add(i, Q, stat);
					System.err.println("Out of Memory!");
					// System.exit(1);
					// continue;
				}

				catch (LimitExceededException e) {
					eva.clear();
					s.numSolns = eva.getTupleCount();
					stat = new QueryEvalStat(s);
					stat.totTime = tt.Stop() / 1000;
					stat.setStatus(status_vals.exceedLimit);
					stats.add(i, Q, stat);
					// e.printStackTrace();
					System.err.println("Exceed Output Limit!");
				}

				catch (Exception e) {
					eva.clear();
					s.numSolns = eva.getTupleCount();
					stat = new QueryEvalStat(s);
					stat.setStatus(status_vals.failure);
					stats.add(i, Q, stat);
					e.printStackTrace();
					System.exit(1);
				}
			}
		}

	}

	private double getTotNodes(Query qry) {

		double totNodes = 0.0;

		QNode[] nodes = qry.nodes;
		for (int i = 0; i < nodes.length; i++) {
			QNode n = nodes[i];
			ArrayList<GraphNode> invLst = invLsts.get(n.lb);
			totNodes += invLst.size();

		}

		return totNodes;
	}

	private void print(Query qry) {
		System.out.println("Size of inverted list per node:");
		for (int i = 0; i < qry.V; i++) {
			QNode q = qry.nodes[i];
			ArrayList<GraphNode> list = invLsts.get(q.lb);
			System.out.println("qid = " + i + " " + " list = " + list.size());
		}
	}

	private void writeStatsToCSV() {
		PrintWriter opw;

		try {
			opw = new PrintWriter(new FileOutputStream(outFileN, true));
			stats.printToFile(opw);
			opw.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	private void checkQueryType(Query query) {

		QEdge[] edges = query.edges;
		query.childOnly = true;
		for (QEdge edge : edges) {
			AxisType axis = edge.axis;
			if (axis == Consts.AxisType.descendant) {

				query.childOnly = false;
				break;
			}

		}

		QueryDirectedCycle finder = new QueryDirectedCycle(query);
		if (!finder.hasCycle()) {
			query.hasCycle = false;
		} else
			query.hasCycle = true;
	}

	public static void main(String[] args) {

		String dataFileN = args[0], queryFileN = args[1]; // the query file
		GraHomFltSimMain demain = new GraHomFltSimMain(dataFileN, queryFileN);

		demain.run();
	}

}
