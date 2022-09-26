package tupleEnumerator;

import java.util.ArrayList;

import dao.Pool;
import dao.PoolEntry;
import dao.Tuple;
import dao.TupleHash;
import global.Consts;
import global.Flags;
import helper.CartesianProduct;
import helper.LimitExceededException;
import query.graph.QNode;
import query.graph.Query;

public class TreeTupleEnumCP {

	ArrayList<Pool> mPool;
	Query mQuery;

	TupleHash[] tupleCache;

	QNode mRoot;
	double mTupleCount;

	public TreeTupleEnumCP(Query qry, ArrayList<Pool> pl) {

		mQuery = qry;
		mPool = pl;

		mRoot = mQuery.getSources().get(0);
		tupleCache = new TupleHash[mQuery.V];

		for (int i = 0; i < mQuery.V; i++) {

			tupleCache[i] = new TupleHash();
		}

	}

	public double getTupleCount() {

		return mTupleCount;
	}

	public void enumTuples() throws LimitExceededException {

		Pool rPool = mPool.get(mRoot.id);
		ArrayList<PoolEntry> elist = rPool.elist();
		mTupleCount = 0;
		for (PoolEntry r : elist) {

			//mTupleCount += joinCount(r);
			mTupleCount += join(r).size();
			if (Flags.OUTLIMIT && mTupleCount >= Consts.OutputLimit) {
				throw new LimitExceededException();
			}

		}

		System.out.println("Total solution tuples:" + mTupleCount);

	}

	private double joinCount(PoolEntry e) {

		QNode q = e.getQNode();
		double count = 1.0;
		if (q.isSink()) {
			return count;
		}
		ArrayList<QNode> children = mQuery.getChildren(q.id);

		for (QNode c : children) {
			ArrayList<PoolEntry> elist = e.getFwdEntries(c.id);
			double tot_c = 0;
			for (int i = 0; i < elist.size(); i++) {
				PoolEntry sub = elist.get(i);
				tot_c += join(sub).size();

			}

			count *= tot_c;

		}

		return count;
	}

	private ArrayList<Tuple> join(PoolEntry e) {

		QNode q = e.getQNode();
		ArrayList<Tuple> solnList = null;

		if (q.isSink()) {

			solnList = new ArrayList<Tuple>(1);
			Tuple t = new Tuple(mQuery.V);
			t.add(q.id, e.getValue().id);
			solnList.add(t);
			return solnList;
		}

		solnList = tupleCache[e.getQID()].getList(e.getValue().id);

		if (solnList != null) {
			return solnList;
		}

		ArrayList<QNode> children = null;

		children = mQuery.getChildren(q.id);

		ArrayList<ArrayList<Tuple>> tupleLists = new ArrayList<ArrayList<Tuple>>(children.size());
		int idx = 0;
		for (QNode c : children) {
			ArrayList<PoolEntry> elist = e.getFwdEntries(c.id);
			ArrayList<Tuple> tupleList = new ArrayList<Tuple>();
			for (int i = 0; i < elist.size(); i++) {
				PoolEntry sub = elist.get(i);
				tupleList.addAll(join(sub));
			}
			tupleLists.add(idx++, tupleList);
		}

		solnList = getCartesianProduct(e, tupleLists);
		tupleCache[e.getQID()].insert(e.getValue().id, solnList);
		// System.out.println("q.id=" + q.id + "," + "#partial tuples =" +
		// solnList.size());

		return solnList;
	}

	private ArrayList<Tuple> getCartesianProduct(PoolEntry e, ArrayList<ArrayList<Tuple>> tupleLists) {

		int num = tupleLists.size();
		long[] lens = new long[num];
		int i = 0;
		for (ArrayList<Tuple> l : tupleLists) {

			lens[i++] = l.size();
		}
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		for (long[] indices : new CartesianProduct(lens)) {
			Tuple t = new Tuple(mQuery.V);
			t.add(e.getQID(), e.getValue().id);
			for (i = 0; i < num; i++) {
				int idx = (int) (indices[i]);
				ArrayList<Tuple> tupleList = tupleLists.get(i);
				Tuple ti = tupleList.get(idx);
				t.set(ti);
			}

			result.add(t);
		}

		return result;
	}

	public static void main(String[] args) {

	}

}
