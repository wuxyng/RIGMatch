package dao;

import java.util.ArrayList;
import java.util.HashMap;

import org.roaringbitmap.RoaringBitmap;

import global.Flags;
import graph.Digraph;
import graph.GraphNode;
import helper.QueryEvalStats;

public class DaoController {

	public ArrayList<ArrayList<GraphNode>> invLsts, invLstsByID;
	public static HashMap<String, Integer> l2iMap;
	public static HashMap<Integer, String> i2lMap;
	public BFLIndex bfl;
	public static Digraph G;
	public ArrayList<RoaringBitmap> bitsByIDArr;
	DigraphLoader loader;
	BFLIndexBuilder bflBuilder;
	QueryEvalStats stats = null;
	GraphNode[] graNodes;

	public DaoController(String fn, QueryEvalStats stats) {

		loader = new DigraphLoader(fn);
		this.stats = stats;
	}

	public DaoController(String fn) {

		loader = new DigraphLoader(fn);
	}

	public GraphNode[] getGraNodes(){
		
		return graNodes;
	}
	
	public void load() {

		loader.loadVE();
		Flags.mt.run();
		G = loader.getGraph();
		System.out.println("graph loaded: " + "V=" + G.V() + " E=" + G.E());
		l2iMap = loader.getL2IMap();
		bitsByIDArr = loader.getBitsIDArr();
		invLstsByID = loader.getInvLsts();
		bflBuilder = new BFLIndexBuilder(G);
		bflBuilder.run();
		Flags.mt.run();
		invLsts = bflBuilder.getInvLsts();
		graNodes  = G.getNodes();
		bfl = new BFLIndex(graNodes);

	}

	public void loadWOBFL() {

		loader.loadVE();
		Flags.mt.run();
		G = loader.getGraph();
		System.out.println("graph loaded: " + "V=" + G.V() + " E=" + G.E());
		l2iMap = loader.getL2IMap();
		bitsByIDArr = loader.getBitsIDArr();
		invLstsByID = loader.getInvLsts();
		graNodes  = G.getNodes();
		stats.setGraphStat(G.V(), G.E(), G.getNumLabels());
		stats.setLoadTime(loader.getLoadTime());
	}

	public void loadGraphAndIndex() {

		load();
		stats.setGraphStat(G.V(), G.E(), G.getNumLabels());
		stats.setLoadTime(loader.getLoadTime());
		stats.setBuildTime(bflBuilder.getBldTime());

	}

	public static void main(String[] args) {

	}

}
