package eu.socialsensor.focused.crawler.bolts;

import static backtype.storm.utils.Utils.tuple;

import java.util.Date;
import java.util.Map;
import java.util.PriorityQueue;

import eu.socialsensor.focused.crawler.models.RankedWebPage;
import eu.socialsensor.framework.common.domain.WebPage;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

public class RankerBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static long avgTimeDiff = 5 * 60 * 1000; // 5 minutes 
	
	private OutputCollector _collector;
	private PriorityQueue<RankedWebPage> _queue;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector = collector;
		this._queue = new PriorityQueue<RankedWebPage>();
		
		Thread[] threads = new Thread[4];
		for(int i=0; i<4; i++) {
			threads[i] = new Thread(new RankerThread(_queue));
			threads[i].start();
		}
	}

	@Override
	public void execute(Tuple input) {
		WebPage webPage = (WebPage) input.getValueByField("webPage");
		if(webPage != null) {
			double score = getScore(webPage);
			RankedWebPage rankedWebPage = new RankedWebPage(webPage, score);
			
			try {
				synchronized(_queue) {
					_queue.offer(rankedWebPage);
				}
			}
			catch(Exception e) {
				System.out.println("Exception: "+e.getMessage());
			}
		}	
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("webPage"));
	}

	private double getScore(WebPage wp) {
		
		int shares = wp.getShares();
		Date date = wp.getDate();
		long publicationTime = date==null? 0L : date.getTime();
		
		double sharesScore = 1 - Math.exp(-0.05 * shares);
		sharesScore = (sharesScore + 1) / 2;
		
		long current = System.currentTimeMillis();
		double pubTimeScore = Math.exp(-(current - publicationTime)/avgTimeDiff);
		pubTimeScore = (pubTimeScore + 1)/2;
		
		return sharesScore * pubTimeScore;
	}
	
	class RankerThread extends Thread {

		PriorityQueue<RankedWebPage> queue;
		
		public RankerThread(PriorityQueue<RankedWebPage> queue) {	
			this.queue = queue;
		}

		public void run() {
			while(true) {
				try {
					RankedWebPage rankedWebPage = null;
					synchronized(_queue) {
						rankedWebPage = queue.poll();
					}
					
					if(rankedWebPage == null) {
						Utils.sleep(500);
					}
					else {
						WebPage webPage = rankedWebPage.getWebPage();
//						System.out.println(webPage.toJSONString());
//						System.out.println(rankedWebPage.toString() + " queue: " + _queue.size());
						synchronized(_collector) {
							_collector.emit(tuple(webPage));
						}
					}
				}
				catch(Exception e) {
					Utils.sleep(500);
					continue;
				}
			}
		};
	}

}
