/**
 * 
 */
package helper;

/**
 * @author xiaoying
 *
 */
public class TimeTracker {

   long start_time, stop_time;
	boolean running;
	
	public TimeTracker(){
		
		running= false;
	}
	
	
	public void Start() {
	      start_time = System.currentTimeMillis();
	      running=true;
	   }
	   
	 public double Stop() {
	     if (!running) return(-1.0);
	     else {
	        stop_time = System.currentTimeMillis();
	        running=false;
	        return (double)(stop_time - start_time);
	     }
	  }
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
