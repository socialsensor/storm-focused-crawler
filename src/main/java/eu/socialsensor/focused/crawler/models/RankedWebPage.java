package eu.socialsensor.focused.crawler.models;

import java.util.PriorityQueue;

import eu.socialsensor.framework.common.domain.WebPage;

public class RankedWebPage implements Comparable<RankedWebPage> {

	private WebPage webPage;
	private double score = 0;
	
	public RankedWebPage(WebPage webPage, double score) {
		this.webPage = webPage;
		this.score = score;
	}

	public WebPage getWebPage() {
		return webPage;
	}
	
	public double getScore() {
		return score;
	}
	
	@Override
	public String toString() {
		return webPage.getUrl() + " : " + score;
	}
	
	public int compareTo(RankedWebPage other) {
		if(this.score > other.score) {
			return -1;
		}
		return 1;
	}

	public static void main(String[] args) {
		PriorityQueue<RankedWebPage> _queue = new PriorityQueue<RankedWebPage>();
		
		RankedWebPage rwp1 = new RankedWebPage(new WebPage("A", "1"), 0.1);
		RankedWebPage rwp2 = new RankedWebPage(new WebPage("B", "1"), 0.2);
		RankedWebPage rwp3 = new RankedWebPage(new WebPage("C", "1"), 0.4);
		RankedWebPage rwp4 = new RankedWebPage(new WebPage("D", "1"), 0.1);
		
		_queue.offer(rwp1);
		_queue.offer(rwp2);
		_queue.offer(rwp3);
		_queue.offer(rwp4);
		
		System.out.println(_queue.poll());
		System.out.println(_queue.poll());
		System.out.println(_queue.poll());
		System.out.println(_queue.poll());	
	}
}
