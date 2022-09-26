package binaryJoin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import org.roaringbitmap.RoaringBitmap;

import dao.BFLIndex;
import dao.Tuple;
import dao.TupleInfo;
import dao.TupleList;
import graph.GraphNode;
import query.graph.QEdge;

public class BinaryJoinOP {

	public BinaryJoinOP() {
	}

	// a SJ b
	public static void semiJoin_left(QEdge e, ArrayList<GraphNode> from, ArrayList<GraphNode> to, BFLIndex bfl){
		
		for (GraphNode s : from) {

			for (GraphNode t : to) {

				if (s.id == t.id)
					continue;
				if (s.L_interval.mEnd < t.L_interval.mStart)
					break;
				if (bfl.reach(s, t) == 1) {

				
				}

			}
		}

		
	}
	
	public static void semiJoin_right(QEdge e, ArrayList<GraphNode> from, ArrayList<GraphNode> to){
		
		
		
	}

	
	// for a/b
	// t_bits is the bitmap of the "to" list
	public static TupleList join(int tupleLen, QEdge e, ArrayList<GraphNode> from, ArrayList<GraphNode> to,
			RoaringBitmap t_bits) {

		TupleInfo schema = new TupleInfo();
		schema.addFields(e);
		TupleList result = new TupleList(schema);

		for (GraphNode s : from) {

			if (s.N_O_SZ == 0)
				continue;
			RoaringBitmap rs_and = RoaringBitmap.and(s.adj_bits_o, t_bits);

			if (rs_and.isEmpty())
				continue;

			for (int ti : rs_and) {
				GraphNode t = to.get(t_bits.rank(ti) - 1);
				Tuple r = new Tuple(tupleLen);
				r.add(e.from, s.id);
				r.add(e.to, t.id);
				result.addTuple(r);
			}

		}

		return result;

	}

	// for a//b
	public static TupleList join(int tupleLen, QEdge e, ArrayList<GraphNode> from, ArrayList<GraphNode> to,
			BFLIndex bfl) {

		TupleInfo schema = new TupleInfo();
		schema.addFields(e);
		TupleList result = new TupleList(schema);

		for (GraphNode s : from) {

			for (GraphNode t : to) {

				if (s.id == t.id)
					continue;
				if (s.L_interval.mEnd < t.L_interval.mStart)
					break;
				if (bfl.reach(s, t) == 1) {

					Tuple r = new Tuple(tupleLen);
					r.add(e.from, s.id);
					r.add(e.to, t.id);
					result.addTuple(r);
				}

			}
		}

		return result;
	}

	public TupleList join(TupleList leftTable, TupleList rightTable, int joinAttributeIndex1, int joinAttributeIndex2) {

		HashMap<Integer, LinkedList<Tuple>> hashTableForJoining = buildHashTable(leftTable, joinAttributeIndex1);
		TupleInfo joinedSchema = buildJoinSchema(leftTable, rightTable);
		TupleList joinedTable = new TupleList(joinedSchema);
		
		for (Tuple rt : rightTable.getList()) {

			LinkedList<Tuple> listOfLeft = hashTableForJoining.get(rt.getValue(joinAttributeIndex2));
			for (Tuple t : listOfLeft) {

				Tuple joinedTuple = new Tuple(t);
				joinedTuple.set(rt);
				joinedTable.addTuple(joinedTuple);
				
			}
		}

		return joinedTable;
	}

	private TupleInfo buildJoinSchema(TupleList leftTable, TupleList rightTable) {

		TupleInfo l_schema = leftTable.getSchema();

		TupleInfo schema = new TupleInfo(l_schema);

		Set<Integer> common = new HashSet<Integer>();

		Iterator<Integer> rightTable_it = rightTable.getSchema().getNodeIDSet().iterator();

		while (rightTable_it.hasNext()) {

			int id = rightTable_it.next();
			if (schema.isField(id)) {

				common.add(id);
			} else {

				schema.addField(id);
			}

		}

		return schema;

	}

	private HashMap<Integer, LinkedList<Tuple>> buildHashTable(TupleList table, int joinAttributeIndex) {

		HashMap<Integer, LinkedList<Tuple>> hashTableForJoining = new HashMap<Integer, LinkedList<Tuple>>();

		LinkedList<Tuple> list = table.getList();

		for (Tuple t : list) {

			LinkedList<Tuple> currentList = hashTableForJoining.get(t.getValue(joinAttributeIndex));

			currentList = (currentList != null) ? currentList : new LinkedList<Tuple>();

			currentList.add(t);

			hashTableForJoining.put(t.getValue(joinAttributeIndex), currentList);

		}

		return hashTableForJoining;
	}

	public static void main(String[] args) {

	}

}
