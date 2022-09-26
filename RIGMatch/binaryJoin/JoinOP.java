package binaryJoin;

import java.util.ArrayList;
import java.util.BitSet;
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
import global.Consts;
import global.Flags;
import graph.GraphNode;
import helper.IntPair;
import helper.LimitExceededException;
import query.graph.QEdge;
import query.graph.QNode;

public class JoinOP {

	long numOutTuples = 0;

	public JoinOP() {
	}

	// for a/b
	// t_bits is the bitmap of the "to" list
	public TupleList join(int tupleLen, QEdge e, ArrayList<GraphNode> from, ArrayList<GraphNode> to,
			RoaringBitmap t_bits) {

		TupleInfo schema = new TupleInfo();
		schema.addFields(e);
		TupleList result = new TupleList(schema);

		for (GraphNode s : from) {

			if (s.N_O_SZ == 0)
				continue;
			RoaringBitmap rs_and = RoaringBitmap.and(s.adj_bits_id_o, t_bits);

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
	public TupleList join(int tupleLen, QEdge e, ArrayList<GraphNode> from, ArrayList<GraphNode> to, BFLIndex bfl) {

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

	public TupleList join(TupleList leftTable, TupleList rightTable) {

		Set<Integer> joinAttributeIndex = new HashSet<Integer>();
		TupleInfo joinedSchema = buildJoinSchema(leftTable, rightTable, joinAttributeIndex);
		TupleList joinedTable = new TupleList(joinedSchema);

		HashMap<Set<Integer>, LinkedList<Tuple>> hashTableForJoining = buildHashTable(leftTable, joinAttributeIndex);

		for (Tuple rt : rightTable.getList()) {

			LinkedList<Tuple> listOfLeft = hashTableForJoining.get(getTupleVals(rt, joinAttributeIndex));
			if (listOfLeft != null)
				for (Tuple t : listOfLeft) {

					Tuple joinedTuple = new Tuple(t);
					joinedTuple.add(rt, joinAttributeIndex);
					joinedTable.addTuple(joinedTuple);

				}
		}

		return joinedTable;
	}
	
	public double getTupleCount() {

		return numOutTuples;
	}


	public void joinFinal(TupleList leftTable, TupleList rightTable) throws LimitExceededException {
		numOutTuples =0;
		Set<Integer> joinAttributeIndex = new HashSet<Integer>();
		TupleInfo joinedSchema = buildJoinSchema(leftTable, rightTable, joinAttributeIndex);
		TupleList joinedTable = new TupleList(joinedSchema);

		HashMap<Set<Integer>, LinkedList<Tuple>> hashTableForJoining = buildHashTable(leftTable, joinAttributeIndex);

		for (Tuple rt : rightTable.getList()) {

			LinkedList<Tuple> listOfLeft = hashTableForJoining.get(getTupleVals(rt, joinAttributeIndex));
		
			//if (listOfLeft != null) {
			//	numOutTuples += listOfLeft.size();
			//	if (numOutTuples >= Consts.OutputLimit)
			//		throw new LimitExceededException();
			//}
			
			
			if (listOfLeft != null)
				for (Tuple t : listOfLeft) {
					
					Tuple joinedTuple = new Tuple(t);
					joinedTuple.add(rt, joinAttributeIndex);
					joinedTable.addTuple(joinedTuple);
					numOutTuples++;
					if (Flags.OUTLIMIT && numOutTuples >= Consts.OutputLimit)
						throw new LimitExceededException();
				}
			
			
		}

	}
	public void joinCount(TupleList leftTable, TupleList rightTable) throws LimitExceededException {
		numOutTuples = 0;
		Set<Integer> joinAttributeIndex = new HashSet<Integer>();
		TupleInfo joinedSchema = buildJoinSchema(leftTable, rightTable, joinAttributeIndex);
		HashMap<Set<Integer>, LinkedList<Tuple>> hashTableForJoining = buildHashTable(leftTable, joinAttributeIndex);

		for (Tuple rt : rightTable.getList()) {

			LinkedList<Tuple> listOfLeft = hashTableForJoining.get(getTupleVals(rt, joinAttributeIndex));
			if (listOfLeft != null) {

				numOutTuples += listOfLeft.size();
				if (Flags.OUTLIMIT && numOutTuples >= Consts.OutputLimit)
					throw new LimitExceededException();
			}
		}

	}
	
	public long getNumOutTuples(){
		
		return numOutTuples;
	}

	public IntPair computeSemiJoinCardinality(QEdge e, TupleList tlist) {

		BitSet s1 = new BitSet(), s2 = new BitSet();

		for (Tuple t : tlist.getList()) {

			s1.set(t.getValue(e.from));
			s2.set(t.getValue(e.to));
		}

		IntPair pair = new IntPair(s1.cardinality(), s2.cardinality());

		return pair;
	}

	public TupleList join(GraphNode[] nodes, QNode[] qnodes, ArrayList<ArrayList<GraphNode>> invLsts,
			TupleList[] edgeTab, BFLIndex bfl, TupleList leftTable, QNode sjNode, boolean isIN, QEdge jedge,
			Set<QEdge> edgeSet_J) {

		if (isIN) {

			ArrayList<QEdge> el = sjNode.E_I;
			Set<QEdge> es = new HashSet<QEdge>(el);
			es.removeAll(edgeSet_J);
			if (!es.isEmpty()) {

				es.add(jedge);
				leftTable = semiJoin(nodes, qnodes, invLsts, bfl, leftTable, sjNode, isIN, jedge, es);
			}
		}

		else {

			ArrayList<QEdge> el = sjNode.E_O;
			Set<QEdge> es = new HashSet<QEdge>(el);
			es.removeAll(edgeSet_J);

			es.removeAll(edgeSet_J);
			if (!es.isEmpty()) {

				es.add(jedge);
				leftTable = semiJoin(nodes, qnodes, invLsts, bfl, leftTable, sjNode, isIN, jedge, es);
			}
		}

		if (edgeSet_J.size() > 1) {

			TupleList rightTab = edgeTab[jedge.eid];
			leftTable = join(leftTable, rightTab);
		}
		return leftTable;

	}

	public TupleList semiJoin(GraphNode[] nodes, QNode[] qnodes, ArrayList<ArrayList<GraphNode>> invLsts, BFLIndex bfl,
			TupleList leftTable, QNode sjNode, boolean isIN, QEdge jedge, Set<QEdge> es) {

		LinkedList<Tuple> listOfLeft = leftTable.getList();

		if (isIN) {

			pruneOneStepTDW(nodes, qnodes, invLsts, bfl, listOfLeft, sjNode, es);

		}

		else {

			pruneOneStepBUP(nodes, qnodes, invLsts, bfl, listOfLeft, sjNode, es);
		}

		return leftTable;
	}

	private void pruneOneStepBUP(GraphNode[] nodes, QNode[] qnodes, ArrayList<ArrayList<GraphNode>> invLsts,
			BFLIndex bfl, LinkedList<Tuple> listOfLeft, QNode q, Set<QEdge> es) {

		for (int i = listOfLeft.size() - 1; i >= 0; i--) {

			GraphNode qn = nodes[listOfLeft.get(i).getValue(q.id)];
			boolean found = pruneOneStepBUP(qnodes, invLsts, bfl, q, es, qn);
			if (!found) {

				listOfLeft.remove(i);
			}

		}
	}

	private boolean pruneOneStepBUP(QNode[] qnodes, ArrayList<ArrayList<GraphNode>> invLsts, BFLIndex bfl, QNode parent,
			Set<QEdge> es, GraphNode gn) {

		for (QEdge o_edge : es) {
			int cid = o_edge.to;
			ArrayList<GraphNode> invlst = invLsts.get(qnodes[cid].id);
			boolean found = false;
			for (GraphNode ni : invlst) {

				if (gn.id == ni.id)
					continue;
				if (gn.L_interval.mEnd < ni.L_interval.mStart) {
					if (!found) {
						return false;
					}
				}

				if (bfl.reach(gn, ni) == 1) {
					found = true;
					break;
				}

			}
			if (!found)
				return false;
		}

		return true;
	}

	private void pruneOneStepTDW(GraphNode[] nodes, QNode[] qnodes, ArrayList<ArrayList<GraphNode>> invLsts,
			BFLIndex bfl, LinkedList<Tuple> listOfLeft, QNode q, Set<QEdge> es) {

		for (int i = listOfLeft.size() - 1; i >= 0; i--) {
			System.out.println(q.id);
			GraphNode qn = nodes[listOfLeft.get(i).getValue(q.id)];
			boolean found = pruneOneStepTDW(qnodes, invLsts, bfl, q, es, qn);
			if (!found) {

				listOfLeft.remove(i);
			}

		}
	}

	/////////////////////////////////////

	////////////////////////////////////

	private boolean pruneOneStepTDW(QNode[] qnodes, ArrayList<ArrayList<GraphNode>> invLsts, BFLIndex bfl, QNode child,
			Set<QEdge> es, GraphNode gn) {

		for (QEdge i_edge : es) {

			int pid = i_edge.from;

			ArrayList<GraphNode> invlst = invLsts.get(qnodes[pid].id);
			boolean found = false;

			for (GraphNode par : invlst) {

				if (gn.id == par.id)
					continue;
				if (bfl.reach(par, gn) == 1) {
					found = true;
				}

				if (found)
					break;

			}

			if (!found)
				return false;
		}

		return true;
	}

	private TupleInfo buildJoinSchema(TupleList leftTable, TupleList rightTable, Set<Integer> joinAttributeIndex) {

		TupleInfo l_schema = leftTable.getSchema();

		TupleInfo schema = new TupleInfo(l_schema);

		Iterator<Integer> rightTable_it = rightTable.getSchema().getNodeIDSet().iterator();

		while (rightTable_it.hasNext()) {

			int id = rightTable_it.next();
			if (schema.isField(id)) {

				joinAttributeIndex.add(id);
			} else {

				schema.addField(id);
			}

		}

		return schema;

	}

	private HashMap<Set<Integer>, LinkedList<Tuple>> buildHashTable(TupleList table, Set<Integer> joinAttributeIndex) {

		HashMap<Set<Integer>, LinkedList<Tuple>> hashTableForJoining = new HashMap<Set<Integer>, LinkedList<Tuple>>();

		LinkedList<Tuple> list = table.getList();

		for (Tuple t : list) {

			Set<Integer> vals = getTupleVals(t, joinAttributeIndex);
			LinkedList<Tuple> currentList = hashTableForJoining.get(vals);

			currentList = (currentList != null) ? currentList : new LinkedList<Tuple>();

			currentList.add(t);

			hashTableForJoining.put(vals, currentList);

		}

		return hashTableForJoining;
	}

	private Set<Integer> getTupleVals(Tuple t, Set<Integer> idx) {

		Set<Integer> vals = new HashSet<Integer>();

		Iterator<Integer> it = idx.iterator();

		while (it.hasNext()) {

			vals.add(t.getValue(it.next()));
		}

		return vals;

	}

	public static void main(String[] args) {

	}

}
