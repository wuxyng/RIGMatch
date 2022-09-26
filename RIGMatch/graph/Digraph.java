package graph;

import java.util.ArrayList;

public class Digraph {

	int V; // number of vertices in this digraph
	int E; // number of edges in this digraph
	GraphNode[] nodes;
	int numLabels = 0;
	
   	
	ArrayList<GraphNode> sources, sinks; // source and sink nodes of the graph

	public Digraph() {
	}

	public Digraph(int V, int E, GraphNode[] nodes) {

		this.V = V;
		this.E = E;
		this.nodes = nodes;

	}

	public void setLables(int lbs) {

		numLabels = lbs;

	}

	public int getNumLabels() {

		return numLabels;
	}

	public GraphNode[] getNodes() {

		return nodes;
	}

	public ArrayList<GraphNode> getSources() {
		if (sources == null) {

			sources = new ArrayList<GraphNode>(5);
			for(GraphNode node:nodes){
				if(node.N_I_SZ==0)
					sources.add(node);
				
			}
			
		}
		
		return sources;
	}

	public ArrayList<GraphNode> getSinks() {
		if (sinks == null) {

			sinks = new ArrayList<GraphNode>(5);
			for(GraphNode node:nodes){
				if(node.N_O_SZ==0)
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
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
