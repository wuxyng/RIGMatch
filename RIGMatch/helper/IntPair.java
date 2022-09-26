package helper;

import java.util.HashSet;
import java.util.Iterator;

//public class IntPair extends Pair<Integer, Integer> {
public class IntPair {

	public int first, second;

	public IntPair() {
	}

	public IntPair(int e1, int e2) {

		first = e1;
		second = e2;

	}

	@Override
	public boolean equals(Object obj) {

		// checking if both the object references are
		// referring to the same object.
		if (this == obj)
			return true;

		// it checks if the argument is of the
		// type Geek by comparing the classes
		// of the passed argument and this object.
		// if(!(obj instanceof Geek)) return false; ---> avoid.
		if (obj == null || obj.getClass() != this.getClass())
			return false;

		// type casting of the argument.
		IntPair ot = (IntPair) obj;

		// comparing the state of argument with
		// the state of 'this' Object.
		return (ot.first == this.first && ot.second == this.second);
	}

	   @Override
	    public int hashCode() { 
	       return first+second; 
	    } 
	
	public String toString() {

		StringBuffer sb = new StringBuffer();

		sb.append("(" + first + "," + second + ")\n");
		return sb.toString();
	}

	public static void main(String[] args) {

		HashSet<IntPair> pairs = new HashSet<IntPair>();

		pairs.add(new IntPair(1, 5));
		pairs.add(new IntPair(2, 3));
		pairs.add(new IntPair(3, 2));
		boolean result = pairs.add(new IntPair(1, 5));

		IntPair p1 = new IntPair(1, 5), p2 = new IntPair(1, 5);
		if (p1.equals(p2))
			System.out.println("yes!");

		Iterator<IntPair> it = pairs.iterator();

		while (it.hasNext()) {

			System.out.println(it.next());
		}

	}

}