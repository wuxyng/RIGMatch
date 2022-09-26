package dao;

import java.util.Hashtable;

import org.roaringbitmap.RoaringBitmap;

public class TupleCache {

	Hashtable<Tuple, RoaringBitmap> ht;

	public TupleCache() {

		ht = new Hashtable<Tuple, RoaringBitmap>();
	}

	public void put(Tuple k, RoaringBitmap v) {

		ht.put(k, v);

	}

	public RoaringBitmap getValue(Tuple k) {

		return ht.get(k);
	}

	public static void main(String[] args) {

		TupleCache tc = new TupleCache();
		Tuple t1 = new Tuple(2);
		RoaringBitmap bits = new RoaringBitmap();
		t1.add(0, 2);
		t1.add(1, 3);
		tc.put(t1, bits );
		Tuple t2 = new Tuple(2);
		
		t2.add(0, 2);
		t2.add(1, 3);
		
		RoaringBitmap bits2 = tc.getValue(t2);
		if(bits2!=null)
			System.out.println("yes!");
		else
			System.out.println("no!");
		
	}

}
