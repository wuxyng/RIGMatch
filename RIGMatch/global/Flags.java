
package global;

import global.Consts.OrderType;
import helper.MemoryTracker;

public class Flags {
	public static boolean DEBUG = false;
	public static boolean OUTLIMIT = true;//false; 
	public static boolean PRUNELIMIT = true; //false; 
	public static OrderType ORDER = OrderType.RI;//OrderType.RI; //OrderType.GQL; //OrderType.SPECIFIED; 
	public static boolean COUNT = false;
	public static boolean sortByCard = true;
	public static int vis_cur = 0; // flag for marking visited nodes, if v.vis == vis_cur, it has been visited.
	public static int REPEATS = 1; // for running the experimental evaluation.
	public static MemoryTracker mt = new MemoryTracker();
	
}
