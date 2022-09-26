package tupleEnumeratorOPT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.RoaringBatchIterator;
import org.roaringbitmap.RoaringBitmap;

import dao.Pool;
import dao.PoolEntry;
import global.Consts;
import global.Consts.DirType;
import global.Flags;
import helper.LimitExceededException;
import helper.QueryEvalStat;
import helper.TimeTracker;
import query.graph.QNode;
import query.graph.Query;
import queryPlan.PlanGenerator;

public class HybTupleEnumer {

	private double tupleCount = 0;
	private Query query;
	private ArrayList<Pool> pool;
	private PoolEntry[] match;
	private int[] order;
	private int[][] bn; // backward neighbors
	private int[] bn_count;
	private TimeTracker tt;

	public HybTupleEnumer(Query qry, ArrayList<Pool> pl) {

		query = qry;
		pool = pl;
		match = new PoolEntry[query.V];
		tupleCount = 0;

		// human CQ3 GFL "0 6 3 1 7 5 2 4";
		// "0 6 1 3 5 4 7 2"; // hu CQ2 GFL
		// "0 3 6 1 7 2 4 5" ; //hu CQ3 BJ
		// "0 6 1 7 2 3 5 4" ; //hu CQ2 BJ
		// "0 2 7 3 5 1 4 6" ; //hu CQ3 CPI
		// "0 1 6 2 3 7 4 5" ; //hu CQ2 CPI
		// "0 1 2 3 4 5 6 7" ; //hu CQ2 CECI
		// "0 2 3 6 7 1 5 4" ; //hu CQ3 CECI

		// BJ ep HQ2,HQ3,HQ4,HQ5,HQ18
		// "0,2,1,4,3" ; // BJ EP HQ2
		// "0,1,4,2,3"; //BJ HQ3
		// "1,3,2,4,0" ;// BJ HQ4
		// "2,3,4,0,1";// BJ HQ5
		// "0,3,2,5,1,4"; //BJ HQ18

		// "2,4,0,1,3" // BJ EM HQ2
		// "1,4,0,2,3" // BJ EM HQ3
		// "1, 3, 4, 2, 0" // BJ EM HQ4
		// "1,2,4,0,5,3"; // BJ EM HQ15
		// "3,5,0,2,4,1" // BJ EM HQ18

		// String str = "0 2 3 6 7 1 5 4" ; //hu CQ3 CECI

		order = // PlanGenerator.generateTopoQueryPlan(query);
				// PlanGenerator.generateRITOPOQueryPlan(query);
				// PlanGenerator.generateRIQueryPlan(query);
				getPlan();
		
		// setPlan(str);
		// PlanGenerator.printSimplifiedQueryPlan(query, order);

		generateBN(query, order);
		tt = new TimeTracker();
	}

	public double enumTuples() throws LimitExceededException {
		int[] count = new int[query.V];
		tt.Start();
		// transition(query.V, 0, stat, count);
		transition(query.V, 0);
		double enumtm = tt.Stop() / 1000;
		System.out.println("Tuple enumeration time:" + enumtm + " sec.");
		System.out.println("Total enumerated solution tuples:" + tupleCount);

		return tupleCount;
	}

	public double getTupleCount() {

		return tupleCount;
	}

	private void transition(int max_depth, int depth) throws LimitExceededException {

		int cur_vertex = order[depth];

		RoaringBitmap candBits = getCandBits(cur_vertex);
		if (candBits.isEmpty()) {

			return;
		}

		Pool pl = pool.get(cur_vertex);
		ArrayList<PoolEntry> elist = pl.elist();

		int[] buffer = new int[256];
		RoaringBatchIterator it = candBits.getBatchIterator();

		while (it.hasNext()) {

			int batch = it.nextBatch(buffer);
			for(int i = 0; i<batch; ++i) {
				PoolEntry e = elist.get(buffer[i]);
				match[cur_vertex] = e;
				if (depth == max_depth - 1) {
					tupleCount++;
					//printMatch();
					// stat.setNumSolns(tupleCount);

					if (Flags.OUTLIMIT && tupleCount >= Consts.OutputLimit)
						throw new LimitExceededException();

				} else {
					transition(max_depth, depth + 1);
				}

				match[cur_vertex] = null;
			}
			

		}

	}

	private void transition(int max_depth, int depth, QueryEvalStat stat) throws LimitExceededException {

		int cur_vertex = order[depth];

		RoaringBitmap candBits = getCandBits(cur_vertex);
		if (candBits.isEmpty()) {

			return;
		}
		

		Pool pl = pool.get(cur_vertex);
		ArrayList<PoolEntry> elist = pl.elist();

		int[] buffer = new int[256];
		RoaringBatchIterator it = candBits.getBatchIterator();

		while (it.hasNext()) {

			int batch = it.nextBatch(buffer);
			for(int i = 0; i<batch; ++i) {
				PoolEntry e = elist.get(buffer[i]);
				match[cur_vertex] = e;
				if (depth == max_depth - 1) {
					tupleCount++;
					// printMatch();
					stat.setNumSolns(tupleCount);
					if (Flags.OUTLIMIT && tupleCount >= Consts.OutputLimit)
						throw new LimitExceededException();

				} else {
					transition(max_depth, depth + 1);
				}

				match[cur_vertex] = null;
			}
			

		}
	
	}

	private void transition(int max_depth, int depth, QueryEvalStat stat, int count[]) throws LimitExceededException {

		int cur_vertex = order[depth];
		QNode qn = query.getNode(cur_vertex);
		RoaringBitmap candBits = getCandBits(cur_vertex);
		if (candBits.isEmpty()) {

			return;
		}

		if (qn.isSink() && !hasFollower(depth)) {

			count[cur_vertex] = candBits.getCardinality();
			if (depth == max_depth - 1) {

				tupleCount += product(count);
				stat.setNumSolns(tupleCount);
				// System.out.println("No. of tuples so far:" + tupleCount);
				if (Flags.OUTLIMIT && tupleCount >= Consts.OutputLimit)
					throw new LimitExceededException();

			} else
				transition(max_depth, depth + 1, stat, count);
		} else {
			count[cur_vertex] = 1;
			Pool pl = pool.get(cur_vertex);
			ArrayList<PoolEntry> elist = pl.elist();

			for (int i : candBits) {
				PoolEntry e = elist.get(i);
				match[cur_vertex] = e;

				if (depth == max_depth - 1) {

					tupleCount += product(count);
					stat.setNumSolns(tupleCount);
					// System.out.println("No. of tuples so far:" + tupleCount);
					if (Flags.OUTLIMIT && tupleCount >= Consts.OutputLimit)
						throw new LimitExceededException();

				} else
					transition(max_depth, depth + 1, stat, count);

				match[cur_vertex] = null;

			}
		}

	}

	private double product(int[] count) {

		double rs = 1;

		for (int c : count) {

			rs *= c;
		}

		return rs;
	}

	private RoaringBitmap getCandBits(int cur_vertex) {

		int num = bn_count[cur_vertex];

		if (num == 0) {
			RoaringBitmap bits = new RoaringBitmap();

			for (PoolEntry e : pool.get(cur_vertex).elist()) {
				bits.add(e.getPos());

			}
			return bits;
		}

		int[] bns = bn[cur_vertex];
		List<RoaringBitmap> orMaps = new ArrayList<>();

		for (int i = 0; i < num; i++) {
			int bn_vertex = bns[i];
			DirType dir = query.dir(bn_vertex, cur_vertex);
			RoaringBitmap curbits;
			PoolEntry bm = match[bn_vertex];
			if (dir == DirType.FWD) {

				curbits = bm.mFwdBits.get(cur_vertex);
			} else {

				curbits = bm.mBwdBits.get(cur_vertex);
			}

			orMaps.add(curbits);
		}

		RoaringBitmap bits = FastAggregation.and(orMaps.iterator());

		return bits;
	}

	private void printMatch() {

		for (PoolEntry v : match) {

			System.out.print(v + " ");
		}

		System.out.println();
	}

	private int[] setPlan(String aString) {

		Pattern p = Pattern.compile("[,:#\\s]+"); // ("[\\s]+");
		// Split input with the pattern
		String[] strArr = p.split(aString);

		int[] order = new int[query.V];

		for (int i = 0; i < query.V; ++i) {
			order[i] = Integer.parseInt(strArr[i]);
		}

		PlanGenerator.printSimplifiedQueryPlan(query, order);

		return order;
	}

	private int[] getPlan() {

		int[] order = null;
		
		if(Flags.ORDER == Consts.OrderType.SPECIFIED)	
			order = query.matchOrder;
		else if (Flags.ORDER == Consts.OrderType.RI)
			order = PlanGenerator.generateRIQueryPlan(query);
		else {

			int[] candidates_count = new int[query.V];

			for (int i = 0; i < query.V; i++) {
				candidates_count[i] = pool.get(i).elist().size();
			}

			if (Flags.ORDER == Consts.OrderType.GQL) {

				order = PlanGenerator.generateGQLQueryPlan(query, candidates_count);
			} else if (Flags.ORDER == Consts.OrderType.RIGQL) {
				
				order = PlanGenerator.generateRIExtQueryPlan(query, candidates_count);
			}
			else
				order = PlanGenerator.generateHybQueryPlan(query, candidates_count);

		}

		PlanGenerator.printSimplifiedQueryPlan(query, order);

		// int[] order = PlanGenerator.generateRIQueryPlan(query);
		// PlanGenerator.printSimplifiedQueryPlan(query, order);

		// int[] order = PlanGenerator.generateHybQueryPlan(query,
		// candidates_count);
		// PlanGenerator.printSimplifiedQueryPlan(query, order);

		// int[] order = PlanGenerator.generateTopoQueryPlan(query);
		// PlanGenerator.printSimplifiedQueryPlan(query, order);

		// int[] order = PlanGenerator.generateRITOPOQueryPlan(query);
		// PlanGenerator.printSimplifiedQueryPlan(query, order);

		return order;
	}

	private boolean hasFollower(int i) {

		int cur_vertex = order[i];

		for (int j = i + 1; j < query.V; j++) {

			int next_vertex = order[j];
			if (query.checkEdgeExistence(cur_vertex, next_vertex))
				return true;
		}
		return false;
	}

	// backward neighbor
	private void generateBN(Query query_graph, int[] order) {
		int query_vertices_num = query_graph.V;
		bn_count = new int[query_vertices_num];
		Arrays.fill(bn_count, 0);
		bn = new int[query_vertices_num][];
		for (int i = 0; i < query_vertices_num; ++i) {
			bn[i] = new int[query_vertices_num];
		}

		boolean[] visited_vertices = new boolean[query_vertices_num];
		Arrays.fill(visited_vertices, false);
		visited_vertices[order[0]] = true;
		for (int i = 1; i < query_vertices_num; ++i) {
			int vertex = order[i];
			ArrayList<Integer> nbrs = query_graph.getNeighborIdList(vertex);
			for (int nbr : nbrs) {
				if (visited_vertices[nbr]) {
					bn[vertex][bn_count[vertex]++] = nbr;
				}
			}

			visited_vertices[vertex] = true;
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
