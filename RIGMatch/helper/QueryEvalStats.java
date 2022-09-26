package helper;

import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import global.Flags;

public class QueryEvalStats {

	public String dataFN, qryFN, algN;
	public int V, E, numLbs;
	public double loadTime = 0.0, bldTime = 0.0;
	public double evalTime;
	public double avgQSize = 0.0;
	// public double totmem = 0.0;
	ArrayList<QueryEvalStat>[] qryEvalStats;
	public double[] totTimes, totMatchTimes, totEnumTimes, totPreTimes, totPlanTimes;

	public QueryEvalStats() {

		qryEvalStats = (ArrayList<QueryEvalStat>[]) new ArrayList[Flags.REPEATS];
		totTimes = new double[Flags.REPEATS];
		totMatchTimes = new double[Flags.REPEATS];
		totEnumTimes = new double[Flags.REPEATS];
		totPreTimes = new double[Flags.REPEATS];
		totPlanTimes = new double[Flags.REPEATS];
	}

	public QueryEvalStats(String dataFN, String qryFN, String algN) {

		qryEvalStats = (ArrayList<QueryEvalStat>[]) new ArrayList[Flags.REPEATS];
		totTimes = new double[Flags.REPEATS];
		totMatchTimes = new double[Flags.REPEATS];
		totEnumTimes = new double[Flags.REPEATS];
		totPreTimes = new double[Flags.REPEATS];
		totPlanTimes = new double[Flags.REPEATS];
		this.dataFN = dataFN;
		this.qryFN = qryFN;
		this.algN = algN;
	}

	public void setGraphStat(int V, int E, int lbs) {

		this.V = V;
		this.E = E;
		numLbs = lbs;

	}

	public void setQryStat(double size) {

		avgQSize = size;
	}

	public void setLoadTime(double lt) {

		loadTime = lt;

	}

	public void setBuildTime(double bt) {

		bldTime = bt;
	}

	public void add(int iter, int qid, double pt, double lt, double mt, double et, double solns) {

		QueryEvalStat qst = new QueryEvalStat(pt, lt, mt, et, solns);
		// add(qst);
		add(iter, qid, qst);
	}

	public void add(int iter, int qid, QueryEvalStat qst) {
		if (qryEvalStats[iter] == null)
			qryEvalStats[iter] = new ArrayList<QueryEvalStat>();

		qryEvalStats[iter].add(qid, qst);

	}

	public void printToFile(PrintWriter opw) {
		opw.append("*****************************************************\r\n");
		opw.append("Dataset:" + dataFN + "\r\n");
		opw.append("V:" + V + " " + "E:" + E + " " + "lbs:" + numLbs + "\r\n");
		opw.append("Queryset:" + qryFN + "\r\n");
		opw.append("Algorithm:" + algN + "\r\n");
		
		DecimalFormat f = new DecimalFormat("##.000");

		int totQs = qryEvalStats[0].size();
		int numQs = totQs;
		double[] evalTimes, matchTimes, enumTimes, preTimes, planTimes;
		evalTimes = new double[numQs];
		matchTimes = new double[numQs];
		enumTimes = new double[numQs];
		preTimes = new double[numQs];
		planTimes = new double[numQs];

		double totNodesBefore = 0.0, totNodesAfter = 0.0;
		double totSolns = 0.0;

		opw.append("id" + " " + "status" + " " + "preTime" + " " + "planTime" + " " + "matchTime" + " " + "enumTime"
				+ " " + "totTime" + " " + "numNodesBefore" + " " + "numNodesAfter" + " " + "numSoln" + " " + "numPlans" + " "
				+ "sizeOfAG"
				+ "\r\n");

		for (int i = 0; i < Flags.REPEATS; i++) {
			numQs = totQs;
			ArrayList<QueryEvalStat> qryEvalStatList = qryEvalStats[i];
			for (int q = 0; q < totQs; q++) {
				QueryEvalStat stat = qryEvalStatList.get(q);
				//if (stat.status != Consts.status_vals.success) {
				//	numQs--;

				//} else {
					matchTimes[q] += stat.matchTime;
					preTimes[q] += stat.preTime;
					planTimes[q] += stat.planTime;
					evalTimes[q] += stat.totTime;
					stat.enumTime = stat.totTime - stat.preTime -stat.planTime- stat.matchTime ;
					enumTimes[q] += stat.enumTime;
					totTimes[i] += evalTimes[q];
					totMatchTimes[i] += matchTimes[q];
					totEnumTimes[i] += enumTimes[q];
					if (i == 0) {
						totNodesBefore += stat.totNodesBefore;
						totNodesAfter += stat.totNodesAfter;
						totSolns += stat.numSolns;
					}
				//}

				opw.append(q + " " + stat.status + " " + f.format(stat.preTime) + " " + f.format(stat.planTime) + " "
						+ f.format(stat.matchTime) + " " + f.format(stat.enumTime) + " " + f.format(stat.totTime) + " "
						+ new DecimalFormat(",###").format(stat.totNodesBefore) + " "
						+ new DecimalFormat(",###").format(stat.totNodesAfter) + " "
						+ new DecimalFormat(",###").format(stat.numSolns) + " "
						+ new DecimalFormat(",###").format(stat.numPlans) + " "
						+ new DecimalFormat(",###").format(stat.sizeOfAnsGraph) + "\r\n");

			}
		}

		opw.append("Average running time: \r\n");
		opw.append("id" + " " + "status" + " " + "preTime" + " " + "planTime" + " " + "matchTime" + " " + "enumTime"
				+ " " + "totTime" + " " + "totNodesBefore" + " " + "totNodesAfter" + " " + "numOfTuples" + " "
				+ "numPlans" + " " +  "sizeOfAG" + "\r\n");
		ArrayList<QueryEvalStat> qryEvalStatList = qryEvalStats[0];
		for (int q = 0; q < totQs; q++) {
			QueryEvalStat stat = qryEvalStatList.get(q);

			opw.append(q + " " + stat.status + " " 
					+ f.format(preTimes[q] / Flags.REPEATS) + " " + f.format(planTimes[q] / Flags.REPEATS) + " "
					+ f.format(matchTimes[q] / Flags.REPEATS) + " " + f.format(enumTimes[q] / Flags.REPEATS) + " "
					+ f.format(evalTimes[q] / Flags.REPEATS) + " "
					+ new DecimalFormat(",###").format(stat.totNodesBefore) + " "
					+ new DecimalFormat(",###").format(stat.totNodesAfter) + " "
					+ new DecimalFormat(",###").format(stat.numSolns) + " "
					+ new DecimalFormat(",###").format(stat.numPlans) + " "
					+ new DecimalFormat(",###").format(stat.sizeOfAnsGraph) + "\r\n");

		}

		opw.append("Data loading Time:" + f.format(loadTime) + "\r\n");
		opw.append("Index building Time:" + f.format(bldTime) + "\r\n");

		double avgExeTime = calAvg(totTimes);
		double avgMatTime = calAvg(totMatchTimes);
		double avgPreTime = calAvg(totPreTimes);
		double avgPlanTime = calAvg(totPlanTimes);
		double avgEnumTime = calAvg(totEnumTimes);

		opw.append("Average Eval Time per run:" + f.format(avgExeTime) + "\r\n");
		opw.append("Average Eval Time per query:" + f.format(avgExeTime / numQs) + "\r\n");
		opw.append("Average pruning Time per run:" + f.format(avgPreTime) + "\r\n");
		opw.append("Average pruning Time per query:" + f.format(avgPreTime / numQs) + "\r\n");
		opw.append("Average Plan Time per run:" + f.format(avgPlanTime) + "\r\n");
		opw.append("Average Plan Time per query:" + f.format(avgPlanTime / numQs) + "\r\n");
		opw.append("Average Matching Time per run:" + f.format(avgMatTime) + "\r\n");
		opw.append("Average Matching Time per query:" + f.format(avgMatTime / numQs) + "\r\n");
		opw.append("Average Enumeration Time per run:" + f.format(avgEnumTime) + "\r\n");
		opw.append("Average Enumeration Time per query:" + f.format(avgEnumTime / numQs) + "\r\n");
		opw.append("Average Nodes before per query:" + f.format(totNodesBefore / numQs) + "\r\n");
		opw.append("Average Nodes after per query:" + f.format(totNodesAfter / numQs) + "\r\n");
		opw.append("Average number of solution tuples per query:" + f.format(totSolns / numQs) + "\r\n");
		opw.append("Max Used Memory:" + f.format(Flags.mt.getMaxUsedMem()) + " MB\r\n");
		opw.append("*****************************************************\r\n");
	
	}

	public String toString() {

		StringBuffer sb = new StringBuffer();

		sb.append("Dataset:" + dataFN + "\r\n");
		sb.append("V:" + V + " " + "E:" + E + " " + "lbs:" + numLbs + "\r\n");
		sb.append("Queryset:" + qryFN + "\r\n");
		sb.append("Algorithm:" + algN + "\r\n");
		DecimalFormat f = new DecimalFormat("##.00");
		int numQs = qryEvalStats[0].size();
		double[] evalTimes, matchTimes, enumTimes, preTimes, planTimes;
		evalTimes = new double[numQs];
		matchTimes = new double[numQs];
		enumTimes = new double[numQs];
		preTimes = new double[numQs];
		planTimes = new double[numQs];

		double totNodesBefore = 0.0, totNodesAfter = 0.0;
		double totSolns = 0.0;

		sb.append("id" + " " + "preTime" + " " + "planTime" + " " + "matchTime" + " " + "enumTime" + " " + "totTime"
				+ " " + "numSoln" + " " + "numNodesBefore" + " " + "numNodesAfter" + "\r\n");

		for (int i = 0; i < Flags.REPEATS; i++) {
			ArrayList<QueryEvalStat> qryEvalStatList = qryEvalStats[i];
			for (int q = 0; q < numQs; q++) {
				QueryEvalStat stat = qryEvalStatList.get(q);
				matchTimes[q] += stat.matchTime;
				enumTimes[q] += stat.enumTime;
				preTimes[q] += stat.preTime;
				planTimes[q] += stat.planTime;
				evalTimes[q] += stat.totTime;
				totTimes[i] += evalTimes[q];
				totMatchTimes[i] += matchTimes[q];
				totEnumTimes[i] += enumTimes[q];
				if (i == 0) {
					totNodesBefore += stat.totNodesBefore;
					totNodesAfter += stat.totNodesAfter;
					totSolns += stat.numSolns;
				}

				sb.append(q + " " + f.format(stat.preTime) + " " + f.format(stat.planTime) + " "
						+ f.format(stat.matchTime) + " " + f.format(stat.enumTime) + " " + f.format(stat.totTime) + " "
						+ new DecimalFormat(",###").format(stat.numSolns) + " "
						+ new DecimalFormat(",###").format(stat.totNodesBefore) + " "
						+ new DecimalFormat(",###").format(stat.totNodesAfter) + " " + "\r\n");

			}
		}

		sb.append("Average running time: \r\n");
		for (int q = 0; q < numQs; q++) {

			sb.append("[q" + " " + q + " " + "preTime" + " " + f.format(preTimes[q] / Flags.REPEATS) + " " + "planTime"
					+ " " + f.format(planTimes[q] / Flags.REPEATS) + " " + "matchTime" + " "
					+ f.format(matchTimes[q] / Flags.REPEATS) + " " + "enumTime" + " "
					+ f.format(enumTimes[q] / Flags.REPEATS) + " " + "totTime" + " "
					+ f.format(evalTimes[q] / Flags.REPEATS) + "]\r\n");
		}

		sb.append("Data loading Time:" + f.format(loadTime) + "\r\n");
		sb.append("Index building Time:" + f.format(bldTime) + "\r\n");
		double avgExeTime = calAvg(totTimes);
		double avgMatTime = calAvg(totMatchTimes);
		double avgPreTime = calAvg(totPreTimes);
		double avgPlanTime = calAvg(totPlanTimes);
		double avgEnumTime = calAvg(totEnumTimes);

		sb.append("Average Eval Time per run:" + f.format(avgExeTime) + "\r\n");
		sb.append("Average Eval Time per query:" + f.format(avgExeTime / numQs) + "\r\n");
		sb.append("Average pruning Time per run:" + f.format(avgPreTime) + "\r\n");
		sb.append("Average pruning Time per query:" + f.format(avgPreTime / numQs) + "\r\n");
		sb.append("Average Plan Time per run:" + f.format(avgPlanTime) + "\r\n");
		sb.append("Average Plan Time per query:" + f.format(avgPlanTime / numQs) + "\r\n");
		sb.append("Average Matching Time per run:" + f.format(avgMatTime) + "\r\n");
		sb.append("Average Matching Time per query:" + f.format(avgMatTime / numQs) + "\r\n");
		sb.append("Average Enumeration Time per run:" + f.format(avgEnumTime) + "\r\n");
		sb.append("Average Enumeration Time per query:" + f.format(avgEnumTime / numQs) + "\r\n");
		sb.append("Average Nodes before per query:" + f.format(totNodesBefore / numQs) + "\r\n");
		sb.append("Average Nodes after per query:" + f.format(totNodesAfter / numQs) + "\r\n");
		sb.append("Average number of solution tuples per query:" + f.format(totSolns / numQs) + "\r\n");
		sb.append("Max Used Memory:" + f.format(Flags.mt.getMaxUsedMem()) + " MB\r\n");

		return sb.toString();
	}

	private double calAvg(double[] statArr) {

		double sum = 0L;
		for (int i = 0; i < Flags.REPEATS; i++) {
			double curr = statArr[i];
			sum += curr;

		}

		return sum / Flags.REPEATS;
	}

	public static void main(String[] args) {

	}

}
