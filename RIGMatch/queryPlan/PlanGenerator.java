package queryPlan;

import java.util.ArrayList;
import java.util.HashSet;

import edu.princeton.cs.algs4.Queue;
import query.graph.QNode;
import query.graph.Query;

public class PlanGenerator {

	
	public static int[] generateRITOPOQueryPlan(Query query) {

		int[] order = new int[query.V];

		boolean[] visited = new boolean[query.V];
		
		int[] indegree = new int[query.V];
		for (int v = 0; v < query.V; v++) {
			indegree[v] = query.indegree(v);
		}
		

		order[0] = 0;
		for (int i = 1; i < query.V; ++i) {
			
			if((indegree[i] < indegree[order[0]]) || 
					(indegree[i] == indegree[order[0]] && query.degree(i) > query.degree(order[0]) )){
				
				
				order[0] = i;
			}
			
		}

		visited[order[0]] = true;
		// Order vertices.
		ArrayList<Integer> tie_vertices = new ArrayList<Integer>(query.V), temp = new ArrayList<Integer>(query.V);

		for (int i = 1; i < query.V; ++i) {
			// Select the vertices with the maximum number of backward
			// neighbors.
			int max_bn = 0;
			for (int u = 0; u < query.V; ++u) {
				if (!visited[u]) {
					// Compute the number of backward neighbors of u.
					int cur_bn = 0;
					for (int j = 0; j < i; ++j) {
						int uu = order[j];
						if (query.checkEdgeExistence(u, uu)) {
							cur_bn += 1;
						}
					}

					// Update the vertices under consideration.
					if (cur_bn > max_bn) {
						max_bn = cur_bn;
						tie_vertices.clear();
						tie_vertices.add(u);
					} else if (cur_bn == max_bn ) {
						tie_vertices.add(u);
					}
				}
			}

			if (tie_vertices.size() != 1) {
				temp.addAll(tie_vertices);
				tie_vertices.clear();

				int count = 0;
				ArrayList<Integer> u_fn = new ArrayList<Integer>(query.V);
				for (int u : temp) {
					// Compute the number of vertices in the matching order that
					// has at least one vertex not in the matching order &&
					// connected with u.
					// Get the neighbors of u that are not in the matching
					// order.

					HashSet<Integer> un = query.getNeighborIds(u);
					for (int j : un) {
						if (!visited[j]) {
							u_fn.add(j);
						}
					}

					// Compute the valid number of vertices.
					int cur_count = 0;
					for (int j = 0; j < i; ++j) {
						int uu = order[j];
						HashSet<Integer> uun = query.getNeighborIds(uu);
						uun.retainAll(u_fn);
						if (uun.size() > 0) {
							cur_count += 1;
						}
					}

					u_fn.clear();

					// Update the vertices under consideration.
					if (cur_count > count) {
						count = cur_count;
						tie_vertices.clear();
						tie_vertices.add(u);
					} else if (cur_count == count) {
						tie_vertices.add(u);
					}
				}
			}

			if (tie_vertices.size() != 1) {
				temp.clear();
				temp.addAll(tie_vertices);
				tie_vertices.clear();

				int count = 0;
				ArrayList<Integer> u_fn = new ArrayList<Integer>(query.V);
				for (int u : temp) {
					// Compute the number of vertices not in the matching order
					// &&
					// not the neighbor of vertices in the matching order, but
					// is connected with u.

					// Get the neighbors of u that are not in the matching
					// order.
					HashSet<Integer> un = query.getNeighborIds(u);
					for (int j : un) {
						if (!visited[j]) {
							u_fn.add(j);
						}
					}

					// Compute the valid number of vertices.
					int cur_count = 0;
					for (int uu : u_fn) {
						boolean valid = true;

						for (int j = 0; j < i; ++j) {
							if (query.checkEdgeExistence(uu, order[j])) {
								valid = false;
								break;
							}
						}

						if (valid) {
							cur_count += 1;
						}
					}

					u_fn.clear();

					// Update the vertices under consideration.
					if (cur_count > count) {
						count = cur_count;
						tie_vertices.clear();
						tie_vertices.add(u);
					} else if (cur_count == count) {
						tie_vertices.add(u);
					}
				}
			}

			
			
			
			order[i] = tie_vertices.get(0);
		
			for (int k = 1; k < tie_vertices.size(); ++k) {
				
				int u = tie_vertices.get(k);
				if((indegree[u] < indegree[order[i]]) || 
						(indegree[u] == indegree[order[i]] && query.degree(u) > query.degree(order[i]) )){
					
					
					order[i] = u;
				}
			}
			visited[order[i]] = true;
			tie_vertices.clear();
			temp.clear();

		}

		return order;
	}

	
	public static int[] generateRITOPOQueryPlan2(Query query) {

		int[] order = new int[query.V];

		boolean[] visited = new boolean[query.V];
		
		int[] indegree = new int[query.V];
		for (int v = 0; v < query.V; v++) {
			indegree[v] = query.indegree(v);
		}
		

		order[0] = 0;
		for (int i = 1; i < query.V; ++i) {
			if ((query.degree(i) > query.degree(order[0]) )|| 
					(query.degree(i) == query.degree(order[0])
					&& indegree[i] < indegree[order[0]])) {
				order[0] = i;
			}
		}

		visited[order[0]] = true;
		// Order vertices.
		ArrayList<Integer> tie_vertices = new ArrayList<Integer>(query.V), temp = new ArrayList<Integer>(query.V);

		for (int i = 1; i < query.V; ++i) {
			// Select the vertices with the maximum number of backward
			// neighbors.
			int max_bn = 0;
			for (int u = 0; u < query.V; ++u) {
				if (!visited[u]) {
					// Compute the number of backward neighbors of u.
					int cur_bn = 0;
					for (int j = 0; j < i; ++j) {
						int uu = order[j];
						if (query.checkEdgeExistence(u, uu)) {
							cur_bn += 1;
						}
					}

					// Update the vertices under consideration.
					if (cur_bn > max_bn) {
						max_bn = cur_bn;
						tie_vertices.clear();
						tie_vertices.add(u);
					} else if (cur_bn == max_bn) {
						tie_vertices.add(u);
					}
				}
			}

			if (tie_vertices.size() != 1) {
				temp.addAll(tie_vertices);
				tie_vertices.clear();

				int count = 0;
				ArrayList<Integer> u_fn = new ArrayList<Integer>(query.V);
				for (int u : temp) {
					// Compute the number of vertices in the matching order that
					// has at least one vertex not in the matching order &&
					// connected with u.
					// Get the neighbors of u that are not in the matching
					// order.

					HashSet<Integer> un = query.getNeighborIds(u);
					for (int j : un) {
						if (!visited[j]) {
							u_fn.add(j);
						}
					}

					// Compute the valid number of vertices.
					int cur_count = 0;
					for (int j = 0; j < i; ++j) {
						int uu = order[j];
						HashSet<Integer> uun = query.getNeighborIds(uu);
						uun.retainAll(u_fn);
						if (uun.size() > 0) {
							cur_count += 1;
						}
					}

					u_fn.clear();

					// Update the vertices under consideration.
					if (cur_count > count) {
						count = cur_count;
						tie_vertices.clear();
						tie_vertices.add(u);
					} else if (cur_count == count) {
						tie_vertices.add(u);
					}
				}
			}

			if (tie_vertices.size() != 1) {
				temp.clear();
				temp.addAll(tie_vertices);
				tie_vertices.clear();

				int count = 0;
				ArrayList<Integer> u_fn = new ArrayList<Integer>(query.V);
				for (int u : temp) {
					// Compute the number of vertices not in the matching order
					// &&
					// not the neighbor of vertices in the matching order, but
					// is connected with u.

					// Get the neighbors of u that are not in the matching
					// order.
					HashSet<Integer> un = query.getNeighborIds(u);
					for (int j : un) {
						if (!visited[j]) {
							u_fn.add(j);
						}
					}

					// Compute the valid number of vertices.
					int cur_count = 0;
					for (int uu : u_fn) {
						boolean valid = true;

						for (int j = 0; j < i; ++j) {
							if (query.checkEdgeExistence(uu, order[j])) {
								valid = false;
								break;
							}
						}

						if (valid) {
							cur_count += 1;
						}
					}

					u_fn.clear();

					// Update the vertices under consideration.
					if (cur_count > count) {
						count = cur_count;
						tie_vertices.clear();
						tie_vertices.add(u);
					} else if (cur_count == count) {
						tie_vertices.add(u);
					}
				}
			}

			order[i] = tie_vertices.get(0);

			visited[order[i]] = true;
			tie_vertices.clear();
			temp.clear();

		}

		return order;
	}

	
	public static int[] generateTopoQueryPlan(Query query) {
		
		int[] order = new int[query.V];
		
		// indegrees of remaining vertices
		int[] indegree = new int[query.V];
		for (int v = 0; v < query.V; v++) {
			indegree[v] = query.indegree(v);
		}

		// initialize
		QNode[] nodes = query.nodes;
		int count = 0;

		// initialize queue to contain all vertices with indegree = 0
		Queue<Integer> queue = new Queue<Integer>();
		for (int v = 0; v < query.V; v++)
			if (indegree[v] == 0)
				queue.enqueue(v);

		for (int j = 0; !queue.isEmpty(); j++) {
			int v = queue.dequeue();
			order[j]=v;
			count++;
			if (nodes[v].N_O_SZ > 0) {
				for (int w : query.getChildrenIDs(v)) {
					indegree[w]--;
					if (indegree[w] == 0)
						queue.enqueue(w);
				}
			}
		}

		// there is a directed cycle in subgraph of vertices with indegree >= 1.
		if (count != query.V) {
			order = null;
		}

				
		return order;
	}
	
	
	public static int[] generateGQLQueryPlan(Query query, int[] candidates_count) {

		/**
		 * Select the vertex v such that (1) v is adjacent to the selected
		 * vertices; and (2) v has the minimum number of candidates.
		 */

		int[] order = new int[query.V];
		boolean[] visited_vertices = new boolean[query.V];
		boolean[] adjacent_vertices = new boolean[query.V];

		int start_vertex = selectGQLStartVertex(query, candidates_count);
		order[0] = start_vertex;
		updateValidVertices(query, start_vertex, visited_vertices, adjacent_vertices);

		for (int i = 1; i < query.V; ++i) {
			int next_vertex = i;
			int min_value = Integer.MAX_VALUE;
			for (int j = 0; j < query.V; ++j) {
				int cur_vertex = j;

				if (!visited_vertices[cur_vertex] && adjacent_vertices[cur_vertex]) {
					if (candidates_count[cur_vertex] < min_value) {
						min_value = candidates_count[cur_vertex];
						next_vertex = cur_vertex;
					} else if (candidates_count[cur_vertex] == min_value
							&& query.degree(cur_vertex) > query.degree(next_vertex)) {
						next_vertex = cur_vertex;
					}
				}
			}
			updateValidVertices(query, next_vertex, visited_vertices, adjacent_vertices);
			order[i] = next_vertex;
		}

		return order;
	}

	
	
	public static int[] generateHybQueryPlan(Query query, int[] candidates_count) {

		int[] order = new int[query.V];

		boolean[] visited = new boolean[query.V];

		order[0] = 0;
		
		
		for (int i = 1; i < query.V; ++i) {

			if ((query.degree(i) > query.degree(order[0])) || (query.degree(i) == query.degree(order[0])
					&& candidates_count[i] < candidates_count[order[0]])) {
				order[0] = i;
			}

		}

		visited[order[0]] = true;
		// Order vertices.
		ArrayList<Integer> tie_vertices = new ArrayList<Integer>(query.V), temp = new ArrayList<Integer>(query.V);

		for (int i = 1; i < query.V; ++i) {
			// Select the vertices with the maximum number of backward
			// neighbors.
			int max_bn = 0;
			for (int u = 0; u < query.V; ++u) {
				if (!visited[u]) {
					// Compute the number of backward neighbors of u.
					int cur_bn = 0;
					for (int j = 0; j < i; ++j) {
						int uu = order[j];
						if (query.checkEdgeExistence(u, uu)) {
							cur_bn += 1;
						}
					}

					// Update the vertices under consideration.
					if (cur_bn > max_bn) {
						max_bn = cur_bn;
						tie_vertices.clear();
						tie_vertices.add(u);
					} else if (cur_bn == max_bn) {
						tie_vertices.add(u);
					}
				}
			}

			if (tie_vertices.size() != 1) {
				temp.addAll(tie_vertices);
				tie_vertices.clear();

				int count = 0;
				ArrayList<Integer> u_fn = new ArrayList<Integer>(query.V);
				for (int u : temp) {
					// Compute the number of vertices in the matching order that
					// has at least one vertex not in the matching order &&
					// connected with u.
					// Get the neighbors of u that are not in the matching
					// order.

					HashSet<Integer> un = query.getNeighborIds(u);
					for (int j : un) {
						if (!visited[j]) {
							u_fn.add(j);
						}
					}

					// Compute the valid number of vertices.
					int cur_count = 0;
					for (int j = 0; j < i; ++j) {
						int uu = order[j];
						HashSet<Integer> uun = query.getNeighborIds(uu);
						uun.retainAll(u_fn);
						if (uun.size() > 0) {
							cur_count += 1;
						}
					}

					u_fn.clear();

					// Update the vertices under consideration.
					if (cur_count > count) {
						count = cur_count;
						tie_vertices.clear();
						tie_vertices.add(u);
					} else if (cur_count == count) {
						tie_vertices.add(u);
					}
				}
			}

			if (tie_vertices.size() != 1) {
				temp.clear();
				temp.addAll(tie_vertices);
				tie_vertices.clear();

				int count = 0;
				ArrayList<Integer> u_fn = new ArrayList<Integer>(query.V);
				for (int u : temp) {
					// Compute the number of vertices not in the matching order
					// && not the neighbor of vertices in the matching order, but
					// is connected with u.

					// Get the neighbors of u that are not in the matching
					// order.
					HashSet<Integer> un = query.getNeighborIds(u);
					for (int j : un) {
						if (!visited[j]) {
							u_fn.add(j);
						}
					}

					// Compute the valid number of vertices.
					int cur_count = 0;
					for (int uu : u_fn) {
						boolean valid = true;

						for (int j = 0; j < i; ++j) {
							if (query.checkEdgeExistence(uu, order[j])) {
								valid = false;
								break;
							}
						}

						if (valid) {
							cur_count += 1;
						}
					}

					u_fn.clear();

					// Update the vertices under consideration.
					if (cur_count > count) {
						count = cur_count;
						tie_vertices.clear();
						tie_vertices.add(u);
					} else if (cur_count == count) {
						tie_vertices.add(u);
					}
				}
			}

			order[i] = tie_vertices.get(0);

			if (tie_vertices.size() != 1) {

				for (int k = 1; k < tie_vertices.size(); k++) {
					int u = tie_vertices.get(k);
					if (candidates_count[u] < candidates_count[order[i]])

						order[i] = u;
				}
			}

			visited[order[i]] = true;
			tie_vertices.clear();
			temp.clear();

		}

		return order;
	}

	private static void updateValidVertices(Query query, int query_vertex, boolean[] visited, boolean[] adjacent) {
		visited[query_vertex] = true;

		HashSet<Integer> nbrs = query.getNeighborIds(query_vertex);
		for (int nbr : nbrs) {

			adjacent[nbr] = true;
		}

	}

	private static int selectGQLStartVertex(Query query, int[] candidates_count) {

		int start_vertex = 0;

		for (int i = 1; i < query.V; i++) {

			int cur_vertex = 1;
			if (candidates_count[cur_vertex] < candidates_count[start_vertex]) {
				start_vertex = cur_vertex;
			} else if (candidates_count[cur_vertex] == candidates_count[start_vertex]
					&& query.degree(cur_vertex) > query.degree(start_vertex)) {
				start_vertex = cur_vertex;
			}

		}

		return start_vertex;
	}

	public static int[] generateRIQueryPlan(Query query) {

		int[] order = new int[query.V];

		boolean[] visited = new boolean[query.V];

		order[0] = 0;
		for (int i = 1; i < query.V; ++i) {
			if (query.degree(i) > query.degree(order[0])) {
				order[0] = i;
			}
		}

		visited[order[0]] = true;
		// Order vertices.
		ArrayList<Integer> tie_vertices = new ArrayList<Integer>(query.V), temp = new ArrayList<Integer>(query.V);

		for (int i = 1; i < query.V; ++i) {
			// Select the vertices with the maximum number of backward
			// neighbors.
			int max_bn = 0;
			for (int u = 0; u < query.V; ++u) {
				if (!visited[u]) {
					// Compute the number of backward neighbors of u.
					int cur_bn = 0;
					for (int j = 0; j < i; ++j) {
						int uu = order[j];
						if (query.checkEdgeExistence(u, uu)) {
							cur_bn += 1;
						}
					}

					// Update the vertices under consideration.
					if (cur_bn > max_bn) {
						max_bn = cur_bn;
						tie_vertices.clear();
						tie_vertices.add(u);
					} else if (cur_bn == max_bn) {
						tie_vertices.add(u);
					}
				}
			}

			if (tie_vertices.size() != 1) {
				temp.addAll(tie_vertices);
				tie_vertices.clear();

				int count = 0;
				ArrayList<Integer> u_fn = new ArrayList<Integer>(query.V);
				for (int u : temp) {
					// Compute the number of vertices in the matching order that
					// has at least one vertex not in the matching order &&
					// connected with u.
					// Get the neighbors of u that are not in the matching
					// order.

					HashSet<Integer> un = query.getNeighborIds(u);
					for (int j : un) {
						if (!visited[j]) {
							u_fn.add(j);
						}
					}

					// Compute the valid number of vertices.
					int cur_count = 0;
					for (int j = 0; j < i; ++j) {
						int uu = order[j];
						HashSet<Integer> uun = query.getNeighborIds(uu);
						uun.retainAll(u_fn);
						if (uun.size() > 0) {
							cur_count += 1;
						}
					}

					u_fn.clear();

					// Update the vertices under consideration.
					if (cur_count > count) {
						count = cur_count;
						tie_vertices.clear();
						tie_vertices.add(u);
					} else if (cur_count == count) {
						tie_vertices.add(u);
					}
				}
			}

			if (tie_vertices.size() != 1) {
				temp.clear();
				temp.addAll(tie_vertices);
				tie_vertices.clear();

				int count = 0;
				ArrayList<Integer> u_fn = new ArrayList<Integer>(query.V);
				for (int u : temp) {
					// Compute the number of vertices not in the matching order
					// &&
					// not the neighbor of vertices in the matching order, but
					// is connected with u.

					// Get the neighbors of u that are not in the matching
					// order.
					HashSet<Integer> un = query.getNeighborIds(u);
					for (int j : un) {
						if (!visited[j]) {
							u_fn.add(j);
						}
					}

					// Compute the valid number of vertices.
					int cur_count = 0;
					for (int uu : u_fn) {
						boolean valid = true;

						for (int j = 0; j < i; ++j) {
							if (query.checkEdgeExistence(uu, order[j])) {
								valid = false;
								break;
							}
						}

						if (valid) {
							cur_count += 1;
						}
					}

					u_fn.clear();

					// Update the vertices under consideration.
					if (cur_count > count) {
						count = cur_count;
						tie_vertices.clear();
						tie_vertices.add(u);
					} else if (cur_count == count) {
						tie_vertices.add(u);
					}
				}
			}

			order[i] = tie_vertices.get(0);

			visited[order[i]] = true;
			tie_vertices.clear();
			temp.clear();

		}

		return order;
	}

	
	
	public static int[] generateRIExtQueryPlan(Query query, int[] candidates_count) {

		int[] order = new int[query.V];

		boolean[] visited = new boolean[query.V];

		order[0] = 0;
		for (int i = 1; i < query.V; ++i) {
			if (query.degree(i) > query.degree(order[0])) {
				order[0] = i;
			}
		}

		visited[order[0]] = true;
		// Order vertices.
		ArrayList<Integer> tie_vertices = new ArrayList<Integer>(query.V), temp = new ArrayList<Integer>(query.V);

		for (int i = 1; i < query.V; ++i) {
			// Select the vertices with the maximum number of backward
			// neighbors.
			int max_bn = 0;
			for (int u = 0; u < query.V; ++u) {
				if (!visited[u]) {
					// Compute the number of backward neighbors of u.
					int cur_bn = 0;
					for (int j = 0; j < i; ++j) {
						int uu = order[j];
						if (query.checkEdgeExistence(u, uu)) {
							cur_bn += 1;
						}
					}

					// Update the vertices under consideration.
					if (cur_bn > max_bn) {
						max_bn = cur_bn;
						tie_vertices.clear();
						tie_vertices.add(u);
					} else if (cur_bn == max_bn) {
						tie_vertices.add(u);
					}
				}
			}

			if (tie_vertices.size() != 1) {
				temp.addAll(tie_vertices);
				tie_vertices.clear();

				int count = 0;
				ArrayList<Integer> u_fn = new ArrayList<Integer>(query.V);
				for (int u : temp) {
					// Compute the number of vertices in the matching order that
					// has at least one vertex not in the matching order &&
					// connected with u.
					// Get the neighbors of u that are not in the matching
					// order.

					HashSet<Integer> un = query.getNeighborIds(u);
					for (int j : un) {
						if (!visited[j]) {
							u_fn.add(j);
						}
					}

					// Compute the valid number of vertices.
					int cur_count = 0;
					for (int j = 0; j < i; ++j) {
						int uu = order[j];
						HashSet<Integer> uun = query.getNeighborIds(uu);
						uun.retainAll(u_fn);
						if (uun.size() > 0) {
							cur_count += 1;
						}
					}

					u_fn.clear();

					// Update the vertices under consideration.
					if (cur_count > count) {
						count = cur_count;
						tie_vertices.clear();
						tie_vertices.add(u);
					} else if (cur_count == count) {
						tie_vertices.add(u);
					}
				}
			}

			if (tie_vertices.size() != 1) {
				temp.clear();
				temp.addAll(tie_vertices);
				tie_vertices.clear();

				int count = 0;
				ArrayList<Integer> u_fn = new ArrayList<Integer>(query.V);
				for (int u : temp) {
					// Compute the number of vertices not in the matching order
					// &&
					// not the neighbor of vertices in the matching order, but
					// is connected with u.

					// Get the neighbors of u that are not in the matching
					// order.
					HashSet<Integer> un = query.getNeighborIds(u);
					for (int j : un) {
						if (!visited[j]) {
							u_fn.add(j);
						}
					}

					// Compute the valid number of vertices.
					int cur_count = 0;
					for (int uu : u_fn) {
						boolean valid = true;

						for (int j = 0; j < i; ++j) {
							if (query.checkEdgeExistence(uu, order[j])) {
								valid = false;
								break;
							}
						}

						if (valid) {
							cur_count += 1;
						}
					}

					u_fn.clear();

					// Update the vertices under consideration.
					if (cur_count > count) {
						count = cur_count;
						tie_vertices.clear();
						tie_vertices.add(u);
					} else if (cur_count == count) {
						tie_vertices.add(u);
					}
				}
			}

			
			order[i] = tie_vertices.get(0);
            //System.out.println(i+ " round tie break:");
			if (tie_vertices.size() != 1) {
				temp.clear();
				temp.addAll(tie_vertices);
				tie_vertices.clear();
				for (int u : temp) {
					//System.out.println("candidates_count[u] =" + candidates_count[u]);
					//System.out.println("candidates_count[order[i]] =" + candidates_count[order[i]]);
					if(candidates_count[u] < candidates_count[order[i]]) {
						
						order[i] = u;
					}
					
				}
				
			}
			
			
			visited[order[i]] = true;
			tie_vertices.clear();
			temp.clear();

		}

		return order;
	}

	public static void printQueryPlan(Query query, int[] order) {

		System.out.printf("Query Plan: ");
		for (int i = 0; i < query.V; ++i) {
			System.out.printf("%d, ", order[i]);
		}
		System.out.printf("\n");

		System.out.printf("%d: N/A\n", order[0]);
		for (int i = 1; i < query.V; ++i) {
			int end_vertex = order[i];
			System.out.printf("%d: ", end_vertex);
			for (int j = 0; j < i; ++j) {
				int begin_vertex = order[j];
				if (query.checkEdgeExistence(begin_vertex, end_vertex)) {
					System.out.printf("R(%d, %d), ", begin_vertex, end_vertex);
				}
			}
			System.out.printf("\n");
		}
	}

	public static void printSimplifiedQueryPlan(Query query, int[] order) {

		System.out.printf("Query Plan: ");
		for (int i = 0; i < query.V; ++i) {
			System.out.printf("%d, ", order[i]);
		}
		System.out.printf("\n");
	}

	public static void main(String[] args) {

	}

}
