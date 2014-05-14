package eu.socialsensor.focused.crawler.utils;

import java.util.LinkedList;

public class Snapshots<E> extends LinkedList<E> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4906198072463980573L;
	private int limit;


	public Snapshots(int n) {
		limit = n;
	}

	@Override
	public boolean add(E v) {
		super.add(v);
		while (size() > limit) { super.remove(); }
		return true;
	}
	
	
}
