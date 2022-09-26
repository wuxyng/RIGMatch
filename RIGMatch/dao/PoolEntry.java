/**
 * 
 */
package dao;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

import org.roaringbitmap.RoaringBitmap;

import graph.GraphNode;
import query.graph.QNode;

/**
 * @author xiaoying
 *
 */
public class PoolEntry implements Comparable<PoolEntry> {

	int mPos;  // position in the pool
	QNode mQNode; // the corresponding query node it matches
	GraphNode mValue;
	public HashMap<Integer, ArrayList<PoolEntry>> mFwdEntries, mBwdEntries;
	public HashMap<Integer, RoaringBitmap> mFwdBits, mBwdBits;
	
    double numChildren = 0; // total number of child entries
    double numParents = 0; // total number of parent entries
    
	double size = 0; // total number of solution tuples for the subquery rooted
						// at mQNode

	public PoolEntry(QNode q, GraphNode val) {
		mQNode = q;
		mValue = val;
		initFBEntries();
	}

	public PoolEntry(int pos, QNode q, GraphNode val) {
		mPos = pos; 
		mQNode = q;
		mValue = val;
		initFBEntries();
	}

	
	public boolean isSink() {

		return mQNode.N_O_SZ == 0;
	}

	public int getPos(){
		
		return mPos;
	}
	
	public int getQID() {

		return mQNode.id;
	}

	public QNode getQNode() {

		return mQNode;
	}

	public ArrayList<PoolEntry> getFwdEntries(int qid) {

		return mFwdEntries.get(qid);
	}
	
	
	public RoaringBitmap getFwdBits(int qid) {

		return mFwdBits.get(qid);
	}
	
	public RoaringBitmap getBwdBits(int qid) {

		return mBwdBits.get(qid);
	}

	public ArrayList<PoolEntry> getBwdEntries(int qid) {

		return mBwdEntries.get(qid);
	}

	
	public void addChild(PoolEntry c) {

		ArrayList<PoolEntry> subs = mFwdEntries.get(c.getQID());
		subs.add(c);
		RoaringBitmap bits = mFwdBits.get(c.getQID());
		bits.add(c.mPos);
		numChildren++;
	}
	
	public void addParent(PoolEntry c) {

		ArrayList<PoolEntry> subs = mBwdEntries.get(c.getQID());
		subs.add(c);
		RoaringBitmap bits = mBwdBits.get(c.getQID());
		bits.add(c.mPos);
		numParents++;
	}


	public GraphNode getValue() {

		return mValue;
	}
	
	public double getNumChildEnties(){
		
		return numChildren;
	}
	
	public double getNumParEnties(){
		
		return numParents;
	}

	public double size() {
		if (size == 0)
			getTotSolns(this);

		return size;
	}

	private void getTotSolns(PoolEntry e) {

		if (e.isSink()) {
			e.size = 1;
			return;
		}

		QNode qn = e.mQNode;
		double tot = 1;
		for (int c : qn.N_O) {
			ArrayList<PoolEntry> elist = e.mFwdEntries.get(c);

			double tot_c = 0;
			for (int i = 0; i < elist.size(); i++) {
				PoolEntry sub = elist.get(i);
				//getTotSolns(sub);
				tot_c += sub.size();
			}
			tot *= tot_c;

		}
		e.size = tot;

	}

	@Override
	public int compareTo(PoolEntry other) {
		int rs = this.mValue.L_interval.mStart - other.mValue.L_interval.mStart;

		return rs;
	}

    public static Comparator<PoolEntry> NodeIDComparator = new Comparator<PoolEntry>(){

		@Override
		public int compare(PoolEntry n1, PoolEntry n2) {
			
			return n1.mValue.id - n2.mValue.id;
			
		}
    	
    	
    };
	
	public String toString() {

		StringBuilder s = new StringBuilder();
		toString(this, s);

		return s.toString();
	}

	private void toStringNested(PoolEntry e, StringBuilder s) {

		if (e.isSink()) {

			s.append(e.mValue.id);
			return;

		}

		s.append(e.mValue.id);

		QNode qn = e.mQNode;
		for (int c : qn.N_O) {
			s.append("{");
			ArrayList<PoolEntry> elist = e.mFwdEntries.get(c);

			for (int i = 0; i < elist.size(); i++) {
				PoolEntry sub = elist.get(i);
				toStringNested(sub, s);
				if (i < elist.size() - 1)
					s.append(",");
			}
			s.append("}");
		}
	}

	private void toString(PoolEntry e, StringBuilder s) {

		s.append(e.mValue.id);
		s.append(" ");
	
	}
	
	private void initFBEntries() {

		if (mQNode.N_O_SZ > 0) {
			int sz = mQNode.N_O_SZ;
			mFwdEntries = new HashMap<Integer, ArrayList<PoolEntry>>(sz);
			mFwdBits = new HashMap<Integer, RoaringBitmap>(sz);
			for (int cid : mQNode.N_O) {

				ArrayList<PoolEntry> subs = new ArrayList<PoolEntry>(1);
				mFwdEntries.put(cid, subs);
				RoaringBitmap bits = new RoaringBitmap();
				mFwdBits.put(cid, bits);
			}
			
		}

		
		if (mQNode.N_I_SZ > 0) {
			int sz = mQNode.N_I_SZ;
			mBwdEntries = new HashMap<Integer, ArrayList<PoolEntry>>(sz);
			mBwdBits = new HashMap<Integer, RoaringBitmap>(sz);
			for (int pid : mQNode.N_I) {

				ArrayList<PoolEntry> subs = new ArrayList<PoolEntry>(1);
				mBwdEntries.put(pid, subs);
				RoaringBitmap bits = new RoaringBitmap();
				mBwdBits.put(pid, bits);
			}
		}
		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		

	}

}
