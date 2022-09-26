package query.graph;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import global.Consts;
import global.Consts.AxisType;


public class TransitiveReduction {

	Query mQuery;
	AxisType[][] pathMatrix;

	public TransitiveReduction(Query query) {
		mQuery = query;
		int n = mQuery.V();
		AxisType[][] original = new AxisType[n][n];

		for (int i = 0; i < n; i++) {
			// Arrays.fill(adjMatrix[i], false);
			Arrays.fill(original[i], AxisType.none);
		}

		// initialize matrix with edges
		QEdge[] edges = mQuery.edges;
		int id = 0;
		for (QEdge edge : edges) {
			int from = edge.from, to = edge.to;
			AxisType axis = edge.axis;
			original[from][to] = axis;
		}

		this.pathMatrix = original;
		transformToPathMatrix(this.pathMatrix);

	}

	public void reduce() {

		int n = pathMatrix.length;
		AxisType[][] transitivelyReducedMatrix = new AxisType[n][n];
		System.arraycopy(pathMatrix, 0, transitivelyReducedMatrix, 0, pathMatrix.length);
		transitiveReduction(transitivelyReducedMatrix);

		HashSet<QEdge> rmSet = new HashSet<QEdge>(n);
		for (int i = 0; i < n; i++) {
			for (int j = 0; j < n; j++) {
				if (transitivelyReducedMatrix[i][j] == AxisType.none) {
					QEdge e = mQuery.getEdge(i, j);
					if (e != null)
						rmSet.add(e);

				}
			}
		}

		mQuery.rmEdges(rmSet);

	}

	private void transformToPathMatrix(AxisType[][] matrix) {
		// compute path matrix
		for (int i = 0; i < matrix.length; i++) {
			for (int j = 0; j < matrix.length; j++) {
				if (i == j) {
					continue;
				}
				if (matrix[j][i] != AxisType.none) {
					for (int k = 0; k < matrix.length; k++) {
						if (matrix[j][k] == AxisType.none && matrix[i][k] != AxisType.none) {
							matrix[j][k] = AxisType.descendant;
						}
					}
				}
			}
		}
	}

	private void transitiveReduction(AxisType[][] pathMatrix) {
		// transitively reduce
		for (int j = 0; j < pathMatrix.length; j++) {
			for (int i = 0; i < pathMatrix.length; i++) {
				if (pathMatrix[i][j] != AxisType.none) {
					for (int k = 0; k < pathMatrix.length; k++) {
						if (pathMatrix[j][k] != AxisType.none && pathMatrix[i][k] == AxisType.descendant) {
							pathMatrix[i][k] = AxisType.none;
						}
					}
				}
			}
		}
	}

	public static void main(String[] args) {
		String queryFN = args[0]; // the query file
		String outFN = args[1]; // the output query file
		String queryFileN = Consts.INDIR + queryFN;
		String outFileN = Consts.OUTDIR + outFN;
		ArrayList<Query> queries = new ArrayList<Query>();
		QueryParser queryParser = new QueryParser(queryFileN);
		Query query = null;
		PrintWriter pw = null;
		try {
			pw = new PrintWriter(new FileOutputStream(outFileN, true));
			/*
			while((query = queryParser.readNextQuery())!=null){
				
				TransitiveReduction tr = new TransitiveReduction(query);
				tr.reduce();
				query.printToFile(pw);
			}
			
			pw.close();
			*/
		} catch (FileNotFoundException e) {

			e.printStackTrace();
		}

		int qid = 0;

		
		while ((query = queryParser.readNextQuery()) != null) {
			// System.out.println(query);
			TransitiveReduction tr = new TransitiveReduction(query);
			tr.reduce();
			System.out.println(query);
			queries.add(query);

			pw.println("q" + qid++);
			// output nodes
		
			for (QNode n : query.nodes) {

				pw.println("v" + " " + n.id + " " + n.lbStr);
			}

			pw.flush();

			// output edges

			for (QEdge e : query.edges) {
				pw.println("e" + " " + e.from + " " + e.to + " " + e.axis.ordinal());

			}

			pw.flush();

		}
	}

	
	
}
