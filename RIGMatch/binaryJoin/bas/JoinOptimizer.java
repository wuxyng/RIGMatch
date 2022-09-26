package binaryJoin.bas;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import edu.princeton.cs.algs4.Queue;
import query.graph.QEdge;
import query.graph.QNode;
import query.graph.Query;

public class JoinOptimizer {

	Query query;
	QEdge[] edges;
	QNode[] nodes;
	int[] edgeCard, nodeCard;
	double[] edgeCost;
	double numPlans =0.0;

	public JoinOptimizer(Query qry, double[] ecost, int[] ecard, int[] ncard) {

		query = qry;
		edges = query.getEdges();
		nodes = query.getNodes();
		edgeCard = ecard;
		nodeCard = ncard;
		edgeCost = ecost;
	}

	public QEdge[] orderJoins(boolean explain) {

		Queue<State> curQueue = enumerateSubplans(edges);
		State lastState = null;

		do {
			HashMap<Set<QEdge>, State> stateHash = new HashMap<Set<QEdge>, State>();
			Queue<State> nextQueue = new Queue<State>();
			while (!curQueue.isEmpty()) {

				State s = curQueue.dequeue();
				computeCostAndCardOfSubplan(s);
				enumerateSubplans(s, stateHash, nextQueue);
				lastState = s;
				numPlans++;
			}
			curQueue = nextQueue;

		} while (!curQueue.isEmpty());

		QEdge[] order = getOrder(lastState, explain);

		return order;
	}

	public double getNumPlans(){
		
		return numPlans;
	}
	
	private void printJoins(State[] states) {

		System.out.println("Total number of generated plans: " + numPlans);
		System.out.println("Join Plan for " + query);

		for (int pos = 1; pos < states.length; pos++) {

			State s = states[pos], p = s.inState;
			QEdge e = s.inEdge;

			System.out.println("Join " + "{" + p.nodeSet + "," + "cost = " + p.cost+ "," + "card = " + p.cardinality + "}" + " with " + e);

		}

	}

	private QEdge[] getOrder(State s, boolean explain) {

		if (s == null)
			return null;

		State[] states = new State[edges.length];
		QEdge[] order = new QEdge[edges.length];

		for (int pos = edges.length - 1; pos >= 0; pos--) {

			order[pos] = s.inEdge;
			states[pos] = s;
			s = s.inState;
		}

		if (explain)
			printJoins(states);
		return order;

	}

	private Queue<QEdge> enumerateCandEdges(State s) {

		Set<Integer> nodeSet = s.nodeSet;
		Set<QEdge> edgeSet = s.edgeSet;
		Queue<QEdge> candEdges = new Queue<QEdge>();

		for (QEdge e : edges) {
			if (!edgeSet.contains(e) && ((nodeSet.contains(e.from) || (nodeSet.contains(e.to))))) {

				candEdges.enqueue(e);
			}

		}

		return candEdges;
	}

	private int computeCostAndCardOfSubplan(State s, State p, QEdge e, boolean dir) {
		// s = p * e
		// let e be a->b
		// dir =1, the joined node is b, dir =0, the joined node is a

		int leftCard = p.cardinality;
		int joinCard = estimateJoinCardinality(leftCard, e, dir);

		// s.setCard(joinCard);

		return joinCard;
	}

	private int estimateJoinCardinality(int leftCard, QEdge e, boolean dir) {

		double ec = edgeCard[e.eid];
		int jc = 0;
		if (dir) {
			jc = nodeCard[e.to];
		} else
			jc = nodeCard[e.from];

		double selectivityFactor = (double)(ec / jc);

		return (int) (leftCard * selectivityFactor);

	}

	private void computeCostAndCardOfSubplan(State s) {

		Set<QEdge> edgeSet = s.edgeSet;
		Set<Integer> nodeSet = s.nodeSet;
		Iterator<QEdge> edgeIt = edgeSet.iterator();
		QEdge inEdge = null;
		State inState = null;

		if (edgeSet.size() == 1) {
			// one-edge subgraph
			QEdge edge = edgeIt.next();
			s.setState(edgeCost[edge.eid], edgeCard[edge.eid], inState, edge);
		} else {

			ArrayList<State> parents = s.parents;
			int bestCard = Integer.MAX_VALUE;
			double bestCost = Double.MAX_VALUE;

			for (State par : parents) {

				QEdge e = getInEdge(par.edgeSet, edgeSet);
				boolean dir = false;
				if (par.nodeSet.contains(e.to))
					dir = true;
				int card = estimateJoinCardinality(par.cardinality, e, dir);
				double cost = par.cost + card;
				if (cost < bestCost) {
					bestCost = cost;
					bestCard = card;
					inEdge = e;
					inState = par;
				}

			}

			s.setState(bestCost, bestCard, inState, inEdge);

		}

	}

	private QEdge getInEdge(Set<QEdge> parSet, Set<QEdge> curSet) {

		Iterator<QEdge> it = curSet.iterator();

		while (it.hasNext()) {

			QEdge e = it.next();
			if (!parSet.contains(e))
				return e;

		}

		return null;
	}

	private Queue<State> enumerateSubplans(QEdge[] edges) {
		// enumerate subplans with one edge

		Queue<State> stateQue = new Queue<State>();

		for (QEdge e : edges) {

			State s = new State();
			s.addEdge(e);
			stateQue.enqueue(s);
			
		}

		return stateQue;
	}

	private void enumerateSubplans(State s, HashMap<Set<QEdge>, State> stateHash, Queue<State> nextQueue) {

		Queue<QEdge> candEdges = enumerateCandEdges(s);
		while (!candEdges.isEmpty()) {
			QEdge e = candEdges.dequeue();
			Set<QEdge> es = new HashSet<QEdge>(s.edgeSet);
			es.add(e);
			State c = stateHash.get(es);
			if (c == null) {

				c = new State(es);
				stateHash.put(es, c);
				nextQueue.enqueue(c);
			}
			c.addParent(s);

		}
	}

	public static void main(String[] args) {

	}

}
