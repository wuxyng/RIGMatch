package query.graph;

import global.Consts;
import global.Consts.AxisType;

public class QEdge {

	public int eid; // edge id
	public int from, to;
	public AxisType axis; // 0:child, 1:descendant

	
	public QEdge(int f, int t) {

		from = f;
		to = t;
		axis = Consts.AxisType.descendant; // descendant by default
	}

	public QEdge(int f, int t, int a ) {

		AxisType ax = AxisType.descendant;
		if(a == 0)
			ax = AxisType.child;
		from = f;
		to = t;
		axis = ax; 
	}

	
	public String toString() {

		StringBuffer s = new StringBuffer();
		s.append("[");
		s.append(from + "," + axis + "," + to + "]");
		return s.toString();
	}
	
	public  int hashCode() {
        int hash = 7;
        int a2i = 0;
        
        switch(axis){
        case child: {a2i = 0;break;}
        
        case descendant:{a2i = 1;break;}
        
        }
        hash = 31 * hash + from;
        hash = 31 * hash + to;
        hash = 31 * hash + a2i;
        
        return hash;
	}
}
