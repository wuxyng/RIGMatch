/**
 * 
 */
package helper;

/**
 * @author xiaoying
 *
 */
public class MemoryTracker {

	private static final long MEGABYTE = 1024L * 1024L;
	private double maxUsedMem=0.0;
	Runtime runtime;
	
	public MemoryTracker(){
		runtime = Runtime.getRuntime();
	}
	
	public void run() {
	  // Calculate the used memory
		double used  = (runtime.totalMemory() - runtime.freeMemory())/MEGABYTE;
		
		if (maxUsedMem<used) 
			maxUsedMem = used; 
	}
	
	public double getMaxUsedMem(){
		
		return maxUsedMem;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
