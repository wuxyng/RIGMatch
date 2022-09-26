package dao;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.roaringbitmap.RoaringBitmap;

import graph.Digraph;
import graph.GraphNode;
import helper.TimeTracker;

public class DigraphLoader {

	private String mFileName;
	private Digraph mG;

	private HashMap<String, Integer> l2iMap;
	private HashMap<Integer, String> i2lMap;

	private int V; // number of vertices in this digraph
	private int E; // number of edges in this digraph

	private int LID = 0;// label id;
	private int numLabels;
	private GraphNode[] nodes; // nodes of the graph

	private ArrayList<RoaringBitmap> bitsByIDArr;
	private ArrayList<ArrayList<GraphNode>> invLstsByID; // ordered by node id

	private double loadTime = 0.0;

	public DigraphLoader(String filename) {

		mFileName = filename;
		l2iMap = new HashMap<String, Integer>();
		i2lMap = new HashMap<Integer, String>();
		numLabels = 0;

	}

	public void loadGRA() {

		BufferedReader in;
		try {
			in = new BufferedReader(new FileReader(mFileName));
			readNextLine(in); // graph_for_greach

			String line = readNextLine(in); // total number of nodes

			if (line == null)
				return;

			V = Integer.parseInt(line);
			E = 0;
			nodes = new GraphNode[V];
			int[] buf = null;

			NodeList[] N_O = new NodeList[V], N_I = new NodeList[V];

			while ((buf = getNextLine(in)) != null) {

				int u = buf[0], v;
				GraphNode node = new GraphNode();
				node.id = u;
				node.lb = 0;
				nodes[u] = node;
				N_O[u] = new NodeList();
				for (int j = 1; j < buf.length; j++) {
					v = buf[j];
					N_O[u].add(v);
					if (N_I[v] == null)
						N_I[v] = new NodeList();
					N_I[v].add(u);
					E++;
				}
			}

			for (int u = 0; u < V; u++) {
				if (N_O[u] != null) {
					nodes[u].N_O_SZ = N_O[u].size();
					// nodes.get(u).N_O = new
					// ArrayList<Integer>(N_O.get(u).size());
					nodes[u].N_O = N_O[u].getList();
				}
				if (N_I[u] != null) {
					nodes[u].N_I_SZ = N_I[u].size();
					// nodes.get(u).N_I = new
					// ArrayList<Integer>(N_I.get(u).size());
					nodes[u].N_I = N_I[u].getList();
				}

			}

			mG = new Digraph(V, E, nodes);
			mG.setLables(1);
			in.close();

		} catch (FileNotFoundException e1) {

			e1.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		}

	}

	public void loadVE() {
		TimeTracker tt = new TimeTracker();
		tt.Start();
		try {
			int[] VE = getVandE();
			V = VE[0];
			E = VE[1];
			nodes = new GraphNode[V];
			bitsByIDArr = new ArrayList<RoaringBitmap>();
			invLstsByID = new ArrayList<ArrayList<GraphNode>>();

			NodeList[] N_O = new NodeList[V], N_I = new NodeList[V];

			BufferedReader in = new BufferedReader(new FileReader(mFileName));
			String line = null;

			while ((line = readNextLine(in)) != null) {
				String[] buf = line.split("\\s+");
				if (line.charAt(0) == 'v') {

					final int index = Integer.parseInt(buf[1]);
					final String label = buf[2];
					GraphNode n = new GraphNode();
					n.id = index;
					Integer lid = l2iMap.get(label);
					RoaringBitmap bits = null;
					ArrayList<GraphNode> invLst = null;
					if (lid == null) {
						n.lb = LID++;
						i2lMap.put(n.lb, label);
						l2iMap.put(label, n.lb);
						bits = new RoaringBitmap();
						bitsByIDArr.add(bits);
						numLabels++;
						invLst = new ArrayList<GraphNode>();
						invLstsByID.add(invLst);
					} else {
						n.lb = lid;
						bits = bitsByIDArr.get(n.lb);
						invLst = invLstsByID.get(n.lb);
					}

					nodes[index] = n;
					nodes[index].adj_bits_id_o = new RoaringBitmap();
					nodes[index].adj_bits_id_i = new RoaringBitmap();
					bits.add(index);
					invLst.add(n);
				}
				if (line.charAt(0) == 'e') {

					final int v = Integer.parseInt(buf[1]);
					final int w = Integer.parseInt(buf[2]);

					nodes[v].adj_bits_id_o.add(w);
					nodes[w].adj_bits_id_i.add(v);
					if (N_O[v] == null) {
						N_O[v] = new NodeList();

					}
					N_O[v].add(w);

					if (N_I[w] == null) {
						N_I[w] = new NodeList();

					}
					N_I[w].add(v);

				}
			} // end of reading graph

			for (int u = 0; u < V; u++) {
				if (N_O[u] != null) {
					nodes[u].N_O_SZ = N_O[u].size();
					// nodes.get(u).N_O = new
					// ArrayList<Integer>(N_O.get(u).size());
					nodes[u].N_O = N_O[u].getList();
				}
				if (N_I[u] != null) {
					nodes[u].N_I_SZ = N_I[u].size();
					// nodes.get(u).N_I = new
					// ArrayList<Integer>(N_I.get(u).size());
					nodes[u].N_I = N_I[u].getList();
				}

			}

			mG = new Digraph(V, E, nodes);
			mG.setLables(numLabels);
			in.close();

		} catch (Exception e) {

			e.printStackTrace();
		}

		loadTime = tt.Stop() / 1000;
	}

	public double getLoadTime() {

		return loadTime;
	}

	public HashMap<String, Integer> getL2IMap() {

		return l2iMap;
	}

	public HashMap<Integer, String> getI2LMap() {

		return i2lMap;
	}

	public Digraph getGraph() {

		return mG;
	}

	public ArrayList<RoaringBitmap> getBitsIDArr() {

		return bitsByIDArr;
	}

	public ArrayList<ArrayList<GraphNode>> getInvLsts() {
		
		
		for(int i=0; i<numLabels; i++){
			
			ArrayList<GraphNode> list=  invLstsByID.get(i);
			//System.out.println("List " + i + " card =" + list.size());
			Collections.sort(list, GraphNode.NodeIDComparator);
		}
		
		return invLstsByID;
	}

	class NodeList {

		ArrayList<Integer> nlist;

		NodeList() {

			nlist = new ArrayList<Integer>();
		}

		ArrayList<Integer> getList() {

			return nlist;
		}

		void add(int n) {

			nlist.add(n);
		}

		int size() {

			return nlist.size();
		}
	}

	private int[] getVandE() throws Exception {
		int[] rs = new int[2];
		BufferedReader bin = new BufferedReader(new FileReader(mFileName));

		String line = null;

		while ((line = readNextLine(bin)) != null) {

			if (line.charAt(0) == 'v')
				rs[0]++;

			if (line.charAt(0) == 'e')
				rs[1]++;

		}

		return rs;
	}

	private int[] getNextLine(BufferedReader in) {

		String line = readNextLine(in);
		if (line != null) {
			// id: ids#
			String[] strArr = splitLine(line);
			int sz = strArr.length;
			int[] buf = new int[sz];
			for (int i = 0; i < sz; ++i) {
				buf[i] = Integer.parseInt(strArr[i]);
			}

			return buf;

		}
		return null;
	}

	private String[] splitLine(String aString) {

		Pattern p = Pattern.compile("[,:#\\s]+"); // ("[\\s]+");
		// Split input with the pattern
		String[] result = p.split(aString);

		return result;

	}

	private static String readNextLine(BufferedReader in) {
		String line = null;

		try {
			// read a non-emtpy line
			while ((line = in.readLine()) != null) {
				line = line.trim();
				if (line.length() > 0)
					break;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return line;
	}

	public static void main(String[] args) {

	}

}
