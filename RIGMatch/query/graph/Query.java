package query.graph;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import global.Consts;
import global.Consts.DirType;

public class Query {

	
	public int Qid;
	public int V, E;
	public QNode[] nodes;
	public QEdge[] edges;
    public boolean childOnly=true; // only child relationships
    public boolean hasCycle = false; 
	
	public int[] matchOrder;
	
	private boolean[][] bwdAdjMatrix, fwdAdjMatrix; //adjMatrix, 
    private QEdge[][] edgeMatrix;
	
	private static int idseq = 0;

	public ArrayList<QNode> sources, sinks; // source and sink nodes of the
											// graph

	public Query(int V, int E, QNode[] nodes, QEdge[] edges) {

		this.V = V;
		this.E = E;
		this.nodes = nodes;
		this.edges = edges;
		Qid = idseq++;
		buildMatrix();
	}

	public Query(int i, int V, int E, QNode[] nodes, QEdge[] edges) {

		this.V = V;
		this.E = E;
		this.nodes = nodes;
		this.edges = edges;
		Qid = i;
		buildMatrix();
	}
	
	public Query(int i, QNode[] nodes, QEdge[] edges) {
		this.nodes = nodes;
		this.edges = edges;
		this.V = nodes.length;
		this.E = edges.length;
		Qid = i;
		buildMatrix();
	}
	

	// the edge direction from u to v
	public DirType dir(int u, int v){
		if(bwdAdjMatrix[u][v])
		   return DirType.BWD;
		else if(fwdAdjMatrix[u][v])
			return DirType.FWD;
		
		return DirType.NOD;
	}
	
	public boolean checkEdgeExistence(int u, int v){
		
		boolean hasEdge = bwdAdjMatrix[u][v] || fwdAdjMatrix[u][v] ;
		
		return hasEdge;
	}

	public QNode[] getNodes() {

		return nodes;
	}

	public QEdge[] getEdges() {

		return edges;
	}

	public int indegree(int id) {

		QNode q = nodes[id];
		return q.N_I_SZ;

	}

	public int degree(int id) {

		QNode q = nodes[id];

		return q.N_I_SZ + q.N_O_SZ;
	}

	
	public void setMatchOrder(int[] mo) {
		
		matchOrder = mo;
	}
	
	public ArrayList<QNode> getNeighbors(int id) {

		ArrayList<QNode> neighbors = new ArrayList<QNode>(V);
		ArrayList<Integer> ids;
		if (nodes[id].N_O_SZ > 0) {

			ids = nodes[id].N_O;
			for (int i : ids) {

				neighbors.add(nodes[i]);
			}

		}

		if (nodes[id].N_I_SZ > 0) {

			ids = nodes[id].N_I;
			for (int i : ids) {

				neighbors.add(nodes[i]);
			}

		}

		return neighbors;
	}

	public ArrayList<Integer> getNeighborIdList(int id) {

		ArrayList<Integer> neighbors = new ArrayList<Integer>(V);

		if (nodes[id].N_O_SZ > 0) {

			neighbors.addAll(nodes[id].N_O);

		}

		if (nodes[id].N_I_SZ > 0) {

			neighbors.addAll(nodes[id].N_I);

		}

		return neighbors;
	}
	
	public HashSet<Integer> getNeighborIds(int id) {

		HashSet<Integer> neighbors = new HashSet<Integer>(V);

		if (nodes[id].N_O_SZ > 0) {

			neighbors.addAll(nodes[id].N_O);

		}

		if (nodes[id].N_I_SZ > 0) {

			neighbors.addAll(nodes[id].N_I);

		}

		return neighbors;
	}

	public ArrayList<QNode> getChildren(int id) {

		ArrayList<QNode> children = new ArrayList<QNode>();
		if (!nodes[id].isSink()) {

			ArrayList<Integer> ids = nodes[id].N_O;

			for (int i : ids) {

				children.add(nodes[i]);
			}
		}

		return children;
	}

	public ArrayList<QNode> getParents(int id) {

		ArrayList<QNode> parents = new ArrayList<QNode>();

		if (!nodes[id].isSource()) {

			ArrayList<Integer> ids = nodes[id].N_I;

			for (int i : ids) {

				parents.add(nodes[i]);
			}
		}

		return parents;
	}

	public QNode getNode(int id) {

		return nodes[id];
	}

	public ArrayList<Integer> getChildrenIDs(int id) {

		return nodes[id].N_O;
	}

	public boolean isTree() {

		for (QNode n : nodes) {

			if (n.N_I_SZ > 1)
				return false;
		}

		if (E != V - 1)
			return false;
		return true;
	}

	public ArrayList<QNode> getSources() {
		if (sources == null) {

			sources = new ArrayList<QNode>(5);
			for (QNode node : nodes) {
				if (node.N_I_SZ == 0)
					sources.add(node);

			}

		}

		return sources;
	}

	public ArrayList<QNode> getSinks() {
		if (sinks == null) {

			sinks = new ArrayList<QNode>(5);
			for (QNode node : nodes) {
				if (node.N_O_SZ == 0)
					sinks.add(node);

			}

		}

		return sinks;
	}

	/**
	 * Returns the number of vertices in this digraph.
	 *
	 * @return the number of vertices in this digraph
	 */
	public int V() {
		return V;
	}

	/**
	 * Returns the number of edges in this digraph.
	 *
	 * @return the number of edges in this digraph
	 */
	public int E() {
		return E;
	}

	public String toString() {
		StringBuilder s = new StringBuilder();
		s.append("Query " + this.Qid +Consts.NEWLINE);
		s.append(V + " vertices, " + E + " edges " + Consts.NEWLINE);
		for (int v = 0; v < V; v++) {
			s.append(String.format("%d: ", v));
			QNode n_v = nodes[v];
			if (n_v.N_O_SZ > 0)
				for (int w : n_v.N_O) {
					s.append(w + " ");
				}
			s.append(Consts.NEWLINE);
		}
		return s.toString();
	}
    
	
	public void printToFile(PrintWriter opw) {
		
	     	opw.println("q #" + this.Qid);
	     	for (QNode n : nodes) {
				opw.println("v" + " " + n.id + " " + n.lb);

			}

			for (QEdge e : edges) {

				opw.println("e" + " " + e.from + " " + e.to + " " + e.axis);
			}
		
	}
	
	public QEdge getEdge(int f, int t){
		
		return edgeMatrix[f][t];
	}
	
	public void rmEdges(HashSet<QEdge> rmSet){
		QEdge[] curedges = new QEdge[E-rmSet.size()]; 
		for(QEdge e:rmSet){
			
			rmEdge(e);
		}
		
		int i=0;
		for(QEdge e:edges){
			if(!rmSet.contains(e)) {
				curedges[i] = e;
				e.eid = i++;
			}
			
		}

		edges = curedges;
	}
	
	private void rmEdge(int f, int t){
		QEdge e = edgeMatrix[f][t];
		if(e==null)
			return;
		fwdAdjMatrix[f][t] = false;
		bwdAdjMatrix[t][f] = false;
		edgeMatrix[f][t] = null;
		QNode fn = nodes[f], tn = nodes[t];
		fn.E_O.remove(e);
		fn.N_O_SZ--;
		fn.N_O.remove(Integer.valueOf(t));
		tn.E_I.remove(e);
		tn.N_I_SZ--;
		tn.N_I.remove(Integer.valueOf(f));
		E--;
	}
	
	private void rmEdge(QEdge e){
		int f=e.from, t = e.to;
		fwdAdjMatrix[f][t] = false;
		bwdAdjMatrix[t][f] = false;
		edgeMatrix[f][t] = null;
		QNode fn = nodes[f], tn = nodes[t];
		fn.E_O.remove(e);
		fn.N_O_SZ--;
		fn.N_O.remove(Integer.valueOf(t));
		tn.E_I.remove(e);
		tn.N_I_SZ--;
		tn.N_I.remove(Integer.valueOf(f));
		E--;
	}
	
	
	private void buildMatrix() {

		//adjMatrix = new boolean[V][V];
		bwdAdjMatrix = new boolean[V][V];
		fwdAdjMatrix = new boolean[V][V];
		edgeMatrix   = new QEdge[V][V]; 
		for (int i = 0; i < V; i++) {
			//Arrays.fill(adjMatrix[i], false);
			Arrays.fill(bwdAdjMatrix[i], false);
			Arrays.fill(fwdAdjMatrix[i], false);
			Arrays.fill(edgeMatrix[i], null);
		}

		for (QEdge e : edges) {

			int f = e.from, t = e.to;

			//adjMatrix[f][t] = true;
			fwdAdjMatrix[f][t] = true;
			//adjMatrix[t][f] = true;
			bwdAdjMatrix[t][f] = true;
			edgeMatrix[f][t] = e;
		}
	}

	
	
	
	public static void main(String[] args) {

	}

}
