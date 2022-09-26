package simfilterOPT;

import java.util.ArrayList;
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
import queryPlan.PlanGenerator;

public class SimFilterNav {

	Query mQuery;
	ArrayList<MatArray> mCandLists;
	BFLIndex mBFL;
	int passNum = 0;
	RoaringBitmap[] mCandBitsArr;
	int[] candidates_count;
	GraphNode[] mGraNodes;

	ArrayList<ArrayList<GraphNode>> mInvLstsByID;
	ArrayList<RoaringBitmap> mBitsByIDArr;

	// AdjMap[][] mFwdAdjMapList, mBwdAdjMapList;
	boolean invLstByQuery = false;

	public SimFilterNav(Query query, GraphNode[] graNodes, ArrayList<ArrayList<GraphNode>> invLstsByID,
			ArrayList<RoaringBitmap> bitsByIDArr, BFLIndex bfl) {

		mQuery = query;
		mBFL = bfl;
		mInvLstsByID = invLstsByID;
		mGraNodes = graNodes;
		mBitsByIDArr = bitsByIDArr;
		init();

	}

	public SimFilterNav(Query query, GraphNode[] graNodes, ArrayList<ArrayList<GraphNode>> invLstsByID,
			ArrayList<RoaringBitmap> bitsByIDArr, BFLIndex bfl, boolean invLstByQuery) {

		mQuery = query;
		mBFL = bfl;
		mInvLstsByID = invLstsByID;
		mGraNodes = graNodes;
		mBitsByIDArr = bitsByIDArr;
		this.invLstByQuery = invLstByQuery;
		init();

	}

	public void prune2() {

		QEdge[] edges = mQuery.edges;
		// QEdge[] edges = getEdges();
		passNum = 0;
		boolean hasChange = backwardCheck(edges);

		do {
			if (Flags.PRUNELIMIT && passNum > Consts.PruneLimit)
				break;

			hasChange = forwardCheck(edges);
			if (hasChange)
				hasChange = backwardCheck(edges);

		} while (hasChange);
		System.out.println("Total passes: " + passNum);
	}

	public void prune() {

		QEdge[] edges = mQuery.edges;
		// QEdge[] edges = getEdges();
		passNum = 0;
		boolean hasChange = false;// = backwardCheck(edges);

		do {
			if (Flags.PRUNELIMIT && passNum > Consts.PruneLimit)
				break;

			boolean backChange = backwardCheck(edges);
			//hasChange = hasChange || forwardCheck(edges);
			boolean forChange= forwardCheck(edges);

			hasChange = backChange||forChange;
			
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

	
	private void filterBwd_c(QNode child, RoaringBitmap candBits) {

		RoaringBitmap union = unionBackAdj(child);
		candBits.and(union);

	}

	private void filterBwd_d(QNode parent, QNode child, ArrayList<GraphNode> candList_p, RoaringBitmap candBits_p) {

		ArrayList<GraphNode> rmvList = new ArrayList<GraphNode>();
		RoaringBitmap rmvBits = new RoaringBitmap();

		for (GraphNode gn : candList_p) {

			boolean found = false;
			MatArray mli = mCandLists.get(child.id);
			for (GraphNode ni : mli.elist()) {

				if (gn.id == ni.id)
					continue;
				if (mBFL.reach(gn, ni) == 1) {
					found = true;
					break;
				}

			}

			if (!found) {
				rmvList.add(gn);
				rmvBits.add(gn.id);

			}

		}

		candBits_p.xor(rmvBits);
		candList_p.removeAll(rmvList);

	}

	private ArrayList<GraphNode> bits2list(RoaringBitmap bits) {

		ArrayList<GraphNode> list = new ArrayList<GraphNode>();
		for (int i : bits) {

			list.add(mGraNodes[i]);
		}

		return list;

	}

	private boolean backwardCheck(QEdge[] edges) {

		boolean hasChange = false;

		for (QEdge e : edges) {
			boolean result = backwardCheck(e);
			hasChange = hasChange || result;
		}
		passNum++;
		return hasChange;
	}

	private boolean backwardCheck(QEdge e) {

		int from = e.from, to = e.to;
		AxisType axis = e.axis;
		QNode child = mQuery.nodes[to], parent = mQuery.nodes[from];
		RoaringBitmap candBits_p = mCandBitsArr[from];
		int card = candBits_p.getCardinality();
		
		ArrayList<GraphNode> rmvList = new ArrayList<GraphNode>();
		RoaringBitmap rmvBits = new RoaringBitmap();
		ArrayList<GraphNode> candList_p = mCandLists.get(parent.id).elist();
		
		for (GraphNode gn : candList_p) {

			boolean found = false;
			MatArray mli = mCandLists.get(child.id);
			for (GraphNode ni : mli.elist()) {

				if (gn.id == ni.id)
					continue;
				if (axis == AxisType.child) {
					
					if (gn.searchOUT(ni.id)) {	
						found = true;
					}
				}
				else if (mBFL.reach(gn, ni) == 1) {
					found = true;
					
				}
				if (found)
					break;
			}

			if (!found) {
				rmvList.add(gn);
				rmvBits.add(gn.id);

			}

		}

		candBits_p.xor(rmvBits);
		candList_p.removeAll(rmvList);
		
		boolean hasChange = card > candBits_p.getCardinality() ? true : false;

		return hasChange;
	}

	private boolean forwardCheck(QEdge[] edges) {

		boolean hasChange = false;
		passNum++;
		for (QEdge e : edges) {
			boolean result = forwardCheck(e);
			hasChange = hasChange || result;
		}

		return hasChange;
	}

	private boolean forwardCheck(QEdge e) {

		int from = e.from, to = e.to;
		AxisType axis = e.axis;
		QNode child = mQuery.nodes[to], parent = mQuery.nodes[from];
		RoaringBitmap candBits_c = mCandBitsArr[child.id];
		int card = candBits_c.getCardinality();
		
			
		ArrayList<GraphNode> rmvList = new ArrayList<GraphNode>();
		RoaringBitmap rmvBits = new RoaringBitmap();

		ArrayList<GraphNode> candList_c = mCandLists.get(child.id).elist();
		
		
		for (GraphNode gn : candList_c) {

			boolean found = false;
			MatArray mli = mCandLists.get(parent.id);
			for (GraphNode par : mli.elist()) {

				if (gn.id == par.id)
					continue;
				
				if (axis == AxisType.child) {
					
					if (par.searchOUT(gn.id)) {	
						found = true;
					}
				}
				else if (mBFL.reach(par, gn) == 1) {
					found = true;
					
				}
				if (found)
					break;
			}

			if (!found) {
				rmvList.add(gn);
				rmvBits.add(gn.id);

			}
		}

		candBits_c.xor(rmvBits);
		candList_c.removeAll(rmvList);
		
		
		boolean hasChange = card > candBits_c.getCardinality() ? true : false;

		return hasChange;
	}

	private void filterFwd_c(QNode parent, RoaringBitmap candBits) {

		RoaringBitmap union = unionFwdAdj(parent);
		candBits.and(union);

	}

	private void filterFwd_d(QNode parent, QNode child, ArrayList<GraphNode> candList_c, RoaringBitmap candBits_c) {

		ArrayList<GraphNode> rmvList = new ArrayList<GraphNode>();
		RoaringBitmap rmvBits = new RoaringBitmap();

		for (GraphNode gn : candList_c) {

			boolean found = false;
			MatArray mli = mCandLists.get(parent.id);
			for (GraphNode par : mli.elist()) {

				if (gn.id == par.id)
					continue;
				if (mBFL.reach(par, gn) == 1) {
					found = true;
					break;
				}

			}

			if (!found) {
				rmvList.add(gn);
				rmvBits.add(gn.id);

			}
		}

		candBits_c.xor(rmvBits);
		candList_c.removeAll(rmvList);
	}

	//////////////////////////////////////////////

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

	private void init() {

		int size = mQuery.V;

		mCandLists = new ArrayList<MatArray>(size);

		mCandBitsArr = new RoaringBitmap[size];
		QNode[] qnodes = mQuery.nodes;

		for (int i = 0; i < size; i++) {
			QNode q = qnodes[i];
			ArrayList<GraphNode> invLst;
			if (invLstByQuery)
				invLst = mInvLstsByID.get(q.id);
			else
				invLst = mInvLstsByID.get(q.lb);
			AdjMap[] adjMap_f = new AdjMap[invLst.size()];
			AdjMap[] adjMap_b = new AdjMap[invLst.size()];

			for (int j = 0; j < invLst.size(); j++) {

				adjMap_f[j] = new AdjMap(mQuery.V);
				adjMap_b[j] = new AdjMap(mQuery.V);

			}

			MatArray mlist = new MatArray();
			mlist.addList(invLst);
			mCandLists.add(q.id, mlist);

			if (invLstByQuery)
				mCandBitsArr[q.id] = mBitsByIDArr.get(q.id);
			else
				mCandBitsArr[q.id] = mBitsByIDArr.get(q.lb).clone();
		}

	}

	private QEdge[] getEdges() {

		// int[] order = PlanGenerator.generateGQLQueryPlan(query,
		// candidates_count);
		// PlanGenerator.printSimplifiedQueryPlan(query, order);

		// int[] order = PlanGenerator.generateRIQueryPlan(query);
		// PlanGenerator.printSimplifiedQueryPlan(query, order);

		// int[] order = PlanGenerator.generateHybQueryPlan(query,
		// candidates_count);
		// PlanGenerator.printSimplifiedQueryPlan(query, order);

		// int[] order = PlanGenerator.generateTopoQueryPlan(query);
		// PlanGenerator.printSimplifiedQueryPlan(query, order);

		int[] order = PlanGenerator.generateRITOPOQueryPlan(mQuery);
		PlanGenerator.printSimplifiedQueryPlan(mQuery, order);

		QEdge[] edges = new QEdge[mQuery.edges.length];
		int k = 0;
		for (int i = 1; i < mQuery.V; ++i) {
			int end_vertex = order[i];
			for (int j = 0; j < i; ++j) {
				int begin_vertex = order[j];
				DirType dir = mQuery.dir(begin_vertex, end_vertex);
				if (dir == DirType.FWD) {

					edges[k++] = mQuery.getEdge(begin_vertex, end_vertex);
				} else if (dir == DirType.BWD) {

					edges[k++] = mQuery.getEdge(end_vertex, begin_vertex);
				}
			}

		}

		return edges;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
