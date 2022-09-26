package dao;

import java.util.Arrays;
import java.util.Set;

public class Tuple {

	int[] entry;

	int len;

	int filled; // number of filled fields

	public Tuple(int len) {

		init(len);

	}

	public Tuple(Tuple o) {

		init(o.len);
		set(o);
	}

	@Override
	public int hashCode() {
	
		final int prime = 31;
		int result = 7;
		for (int i = 0; i < len; i++) {

		
			result = prime *result  + entry[i];

			// System.out.print(match[bns[i]].getValue().id + ",");
		}
		// System.out.println(result);
		return result;
	}

	@Override
	public boolean equals(Object o) {

		// If the object is compared with itself then return true
		if (o == this) {
			return true;
		}

		/*
		 * Check if o is an instance of Tuple or not "null instanceof [type]"
		 * also returns false
		 */
		if (!(o instanceof Tuple)) {
			return false;
		}

		// typecast o to Tuple so that we can compare data members
		Tuple t = (Tuple) o;

		for (int i = 0; i < len; i++) {
			if (entry[i] != t.entry[i])
				return false;

		}

		return true;
	}

	private void init(int len) {

		entry = new int[len];
		this.len = len;
		Arrays.fill(entry, -1);
		filled = 0;

	}

	public int getValue(int idx) {

		return entry[idx];
	}

	public void set(Tuple o) {

		for (int i = 0; i < len; i++) {

			if (o.entry[i] != -1) {

				entry[i] = o.entry[i];
			}
		}

		filled = o.filled;
	}

	public void add(Tuple o, Set<Integer> common) {

		for (int i = 0; i < len; i++) {

			if (o.entry[i] != -1 && !common.contains(i)) {

				entry[i] = o.entry[i];
				filled++;
			}
		}
	}

	public void add(Tuple o, int common) {

		for (int i = 0; i < len; i++) {

			if (o.entry[i] != -1 && i!=common) {

				entry[i] = o.entry[i];
				filled++;
			}
		}
	}
	
	public void add(int idx, int val) {

		entry[idx] = val;
		filled++;
	}

	public void destroy() {

		entry = null;
		filled = 0;
	}

	public int hasMatched() {

		return filled;
	}

	public int getLength() {

		return len;
	}

	public String toString() {

		StringBuilder s = new StringBuilder();

		for (int v : entry) {

			s.append(v + " ");
		}

		return s.toString();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
