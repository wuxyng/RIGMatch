package answerGraphOPT;

import java.util.ArrayList;

import org.roaringbitmap.RoaringBatchIterator;
import org.roaringbitmap.RoaringBitmap;

import dao.Pool;
import dao.PoolEntry;
import graph.GraphNode;
import query.graph.QEdge;
import query.graph.QNode;
import query.graph.Query;

public class EdgeAnsGraphBuilder {

	Query mQuery;
	ArrayList<Pool> mPool;
	RoaringBitmap[] mCandBitsArr;
	GraphNode[] mGraNodes;

	public EdgeAnsGraphBuilder(Query query, GraphNode[] graNodes, RoaringBitmap[] candBitsArr) {

		mQuery = query;
		mCandBitsArr = candBitsArr;
		mGraNodes = graNodes;
	}

	public ArrayList<Pool> run() {

		initPool();

		for (QEdge edge : mQuery.edges) {

			linkOneStep(edge);
		}

		return mPool;

	}

	private void initPool() {

		mPool = new ArrayList<Pool>(mQuery.V);
		QNode[] qnodes = mQuery.nodes;
		for (int i = 0; i < mQuery.V; i++) {
			Pool qAct = new Pool();
			mPool.add(qAct);
			RoaringBitmap bits = mCandBitsArr[i];
			QNode qn = qnodes[i];
			int pos = 0;
			for (int j : bits) {
				GraphNode n = mGraNodes[j];
				PoolEntry actEntry = new PoolEntry(pos++, qn, n);
				qAct.addEntry(actEntry);
			}

		}

	}

	private void linkOneStep(QEdge edge) {

		int from = edge.from, to = edge.to;
		Pool pl_f = mPool.get(from), pl_t = mPool.get(to);

		for (PoolEntry e_f : pl_f.elist()) {
			linkOneStep(e_f, mCandBitsArr[to], pl_t.elist());
		
		}

	}


	private boolean linkOneStep(PoolEntry r, RoaringBitmap t_bits, ArrayList<PoolEntry> list) {

		GraphNode s = r.getValue();

		RoaringBitmap rs_and = RoaringBitmap.and(s.adj_bits_id_o, t_bits);

		if (rs_and.isEmpty())
			return false;

		RoaringBatchIterator it = rs_and.getBatchIterator();
		int[] buffer = new int[256];

		while (it.hasNext()) {

			int batch = it.nextBatch(buffer);
			for(int i = 0; i<batch; ++i) {
				PoolEntry e = list.get(t_bits.rank(buffer[i]) - 1);
				r.addChild(e);
				e.addParent(r);
				
			}

		}

		return true;

	}


	public static void main(String[] args) {

	}

}
