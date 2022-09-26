package dao;

import java.util.ArrayList;

import graph.GraphNode;

public class MatArray {

	ArrayList<GraphNode> elist;

	public MatArray() {

		elist = new ArrayList<GraphNode>();
	}

	public ArrayList<GraphNode> elist() {

		return elist;
	}

	public void addEntry(GraphNode entry) {

		elist.add(entry);

	}

	public void addList(ArrayList<GraphNode> lst) {

		for (GraphNode e : lst) {

			addEntry(e);
		}

	}

	public void setList(ArrayList<GraphNode> lst) {

		elist.clear();
		elist = lst;
	}

	public void clear() {

		elist.clear();
	}

	public static void main(String[] args) {

	}

}
