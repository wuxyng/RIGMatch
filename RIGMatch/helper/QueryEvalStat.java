package helper;

import java.util.ArrayList;

import dao.Pool;
import dao.PoolEntry;
import global.Consts.status_vals;

public class QueryEvalStat {

	public double sizeOfAnsGraph = 0.0, numSolns = 0.0, totNodesBefore = 0.0, totNodesAfter = 0.0, totSolnNodes = 0.0;
	public double preTime = 0.0, planTime = 0.0, matchTime = 0.0, enumTime = 0.0,  totTime = 0.0;
    public status_vals status = status_vals.success;
	public double numPlans = 0.0;
	
	public QueryEvalStat() {
	}

	
	public QueryEvalStat(QueryEvalStat s){
		
		setFields(s.preTime, s.planTime, s.matchTime, s.enumTime, s.numSolns, s.numPlans, s.sizeOfAnsGraph);
		setTotNodesBefore(s.totNodesBefore);
		setTotNodesAfter(s.totNodesAfter);
	}
	
	public QueryEvalStat(double pt, double lt, double mt, double et, double solns) {
		setFields(pt, lt, mt, et, solns, 0.0, 0.0);
	}
	
	public QueryEvalStat(double pt, double lt, double mt, double et, double solns, double pls) {
	       
		setFields(pt, lt, mt, et, solns, pls, 0.0);
	}
	
	private void setFields(double pt, double lt, double mt, double et, double solns, double pls, double agsz){
		
		preTime = pt;
		planTime = lt;
		matchTime = mt;
		enumTime = et;
		totTime = mt + et + pt + lt;
		numSolns = solns;
		numPlans = pls;
		sizeOfAnsGraph = agsz;
	}
	
	public void setTotNodesBefore(double n){
		
		totNodesBefore = n;
	}
	
	public void setTotNodesAfter(double n){
		
		totNodesAfter = n;
	}
	
	public void setPreTime(double pt){
		
		preTime = pt;
		totTime+=pt;
	}
	
	
	public void setPlanTime(double lt){
		planTime = lt;
		totTime+=lt;
		
	}
	
	public void setMatchTime(double mt){
		matchTime = mt;
		totTime+=mt;
		
	}
	
	public void setEnumTime(double et){
		
		enumTime = et;
		totTime+=et;
	}

	public void setNumSolns(double solns){
		
		numSolns = solns;
	}
	
	public void setNumPlans(double pls){
		
		numPlans = pls;
	}
	
	public void setStatus(status_vals s){
		
		status = s;
	
	}

	public void calAnsGraphSize(ArrayList<Pool> poolArr){
	
		for(Pool pl:poolArr){
			sizeOfAnsGraph +=pl.elist().size(); //nodes
			for (PoolEntry e :pl.elist()){
				sizeOfAnsGraph+=e.getNumChildEnties(); //out edges
				//sizeOfAnsGraph+=e.getNumParEnties(); //in edges
			}
			
		}
		
		
	}

	public static void main(String[] args) {

	}

}
