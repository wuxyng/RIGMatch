package helper;

import java.util.HashSet;
import java.util.Iterator;

//public class IntPair extends Pair<Integer, Integer> {
public class IntTriple {

	public int first, second, third;

	public IntTriple() {
	}

	public IntTriple(int e1, int e2, int e3) {

		first = e1;
		second = e2;
		third = e3;

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
		IntTriple ot = (IntTriple) obj;

		// comparing the state of argument with
		// the state of 'this' Object.
		return (ot.first == this.first && ot.second == this.second && ot.third == this.third);
	}

	@Override
	public int hashCode() {
		return first + second + third;
	}

	public String toString() {

		StringBuffer sb = new StringBuffer();

		sb.append("(" + first + "," + second + "," + third + ")\n");
		return sb.toString();
	}

	public static void main(String[] args) {

	

	}

}