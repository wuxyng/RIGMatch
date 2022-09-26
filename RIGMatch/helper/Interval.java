/**
 * 
 */
package helper;


/**
 * @author xiaoying
 *
 */
public class Interval {

	public int mStart;
	public int mEnd;
	
	public Interval() {
	}

	public Interval(int s, int e) {

		mStart = s;
		mEnd = e;
		
	}

	public int getStart() {
		return mStart;
	}

	public void setStart(int start) {
		mStart = start;
	}

	public int getEnd() {
		return mEnd;
	}

	public void setEnd(int end) {
		mEnd = end;
	}

	public boolean equals(Object other) {
		if (other == this)
			return true;
		if (other == null)
			return false;
		if (other.getClass() != this.getClass())
			return false;
		Interval that = (Interval) other;
		return this.mStart == that.mStart && this.mEnd == that.mEnd;
	}

	public int compareTo(Object other) {

		if (!(other instanceof Interval)) {

			throw new ClassCastException("Invalid interval");

		}

		if (this.mStart > ((Interval) other).getEnd()) {
			return 1; // THIS is to the right of OTHER
		} else if (this.mStart <= ((Interval) other).getStart() && this.mEnd >= ((Interval) other).getEnd())
			return 0;// THIS is an ancestor of OTHER
		else if (this.mEnd < ((Interval) other).getStart())// THIS is to the
															// left of OTHER
			return -1;
		return -2;

	}

	public int hashCode() {
		int hash1 = ((Integer) mStart).hashCode();
		int hash2 = ((Integer) mEnd).hashCode();
		return 31 * hash1 + hash2;
	}

	public String toString() {

		StringBuffer s = new StringBuffer();
		s.append("[");
		s.append("START: " + mStart + ",");
		s.append("END: " + mEnd + "]");
		return s.toString();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
	
	}

}
