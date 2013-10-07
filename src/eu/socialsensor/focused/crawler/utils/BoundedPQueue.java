package eu.socialsensor.focused.crawler.utils;

import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

public class BoundedPQueue<E extends Comparable<E>> {
	

	public static void main(String...args) {
		
	}
	
	/**
 	* Lock used for all public operations
 	*/
	private final ReentrantLock lock;

	private PriorityBlockingQueue<E> queue ;
	private int size = 0;

	public BoundedPQueue(int capacity) {
		queue = new PriorityBlockingQueue<E>(capacity, new CustomComparator<E>());
		size = capacity;
		this.lock = new ReentrantLock();
	}

	public boolean offer(E e) {

		//final ReentrantLock lock = this.lock;
		lock.lock();
		E vl = null;
		if(queue.size() >= size)  {
			vl = poll();
			if(vl.compareTo(e)<0)
				e = vl;
		}
		
		boolean b;
		try {
			b = queue.offer(e);
		} 
		finally {
			lock.unlock();
		}
		return b;
	}

	public E poll()  {
		return queue.poll();
	}
	
	public int size() {
		return queue.size();
	}
	
	public static class CustomComparator<E extends Comparable<E>> implements Comparator<E> {
		@Override
		public int compare(E o1, E o2) {
			//give me a max heap
			return o1.compareTo(o2) * -1;
		}
	}

}
