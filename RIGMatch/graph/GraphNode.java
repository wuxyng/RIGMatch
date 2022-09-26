package graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.roaringbitmap.RoaringBitmap;

import global.Consts;
import helper.Interval;

public class GraphNode implements Comparable<GraphNode> {

	public int N_O_SZ = 0, N_I_SZ = 0;
	public ArrayList<Integer> N_O, N_I;
	public Interval L_interval;
	public int[] L_in;
	public int[] L_out;
	public Integer h_in, h_out; // unsigned int
	public int id; // node id
	public int vis; // visiting flag
	public int lb; // label id
	public int pos; // position in its inverted list

	public RoaringBitmap adj_bits_o; //outgoing adjacency nodes by node's start
	public RoaringBitmap adj_bits_i; //incoming adjacency nodes by node's start
	public RoaringBitmap adj_bits_id_o; //outgoing adjacency nodes by node's id
	public RoaringBitmap adj_bits_id_i; //incoming adjacency nodes by node's id
	
	private boolean outsorted = false;

	public GraphNode() {

		L_interval = new Interval();
		L_in = new int[Consts.K];
		L_out = new int[Consts.K];
		
	}

	public GraphNode(int id, int lb, Interval iv) {

		this.id = id;
		this.lb = lb;
		L_interval = iv;

	}

	// find id in the N_O
	public boolean linearSearchOUT(int id) {

		int rs = linear_4(id);
		return rs < N_O_SZ ? true : false;

	}

	public boolean searchOUT(int id) {
		if (N_O_SZ == 0)
			return false;
		if (N_O_SZ<64)
			return linearSearchOUT(id);
		if (!outsorted) {
			Collections.sort(N_O);
			outsorted = true;
		}
		int rs = Collections.binarySearch(N_O, id);
		return rs < N_O_SZ && rs >= 0 ? true : false;
	}

	private int linear_4(int id) {
		int i = 0;
		while (i + 3 < N_O_SZ) {

			if (N_O.get(i + 0) == id)
				return i + 0;
			if (N_O.get(i + 1) == id)
				return i + 1;
			if (N_O.get(i + 2) == id)
				return i + 2;
			if (N_O.get(i + 3) == id)
				return i + 3;
			i += 4;
		}

		while (i < N_O_SZ) {

			if (N_O.get(i) == id)
				break;
			++i;
		}

		return i;
	}

	@Override
	public int compareTo(GraphNode o) {

		int rs = this.L_interval.mStart - o.L_interval.mStart;

		return rs;
	}

	public static Comparator<GraphNode> NodeIDComparator = new Comparator<GraphNode>() {

		@Override
		public int compare(GraphNode n1, GraphNode n2) {

			return n1.id - n2.id;

		}

	};

	public String toString() {

		StringBuilder s = new StringBuilder();

		s.append("[" + id + "," + lb + "," + pos + "," + "(" + L_interval.mStart + " " + L_interval.mEnd + ")]");
		return s.toString();
	}

	public static void main(String[] args) {
		// System.out.println(ObjectSizeFetcher.getObjectSize(new GraphNode()));

	}
}
