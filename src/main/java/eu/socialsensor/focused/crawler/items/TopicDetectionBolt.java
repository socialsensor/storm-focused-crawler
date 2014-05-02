package eu.socialsensor.focused.crawler.items;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.Item;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class TopicDetectionBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -949601171375801856L;

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	private Logger _logger;
	private BlockingQueue<Item> _queue;
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {

		this._logger = Logger.getLogger(EntityExtractionBolt.class);
		this._queue = new LinkedBlockingQueue<Item>(5000);
		
		Thread t = new Thread(new TopicDetectionTask(_queue));
		t.start();
	}

	public void execute(Tuple input) {
		try {
			Item item = (Item) input.getValueByField("Item");
			if(item == null)
				return;
		
			_queue.put(item);
		}
		catch(Exception e) {
			_logger.error(e);
		}
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	public class TopicDetectionTask implements Runnable {

		private BlockingQueue<Item> queue;
		
		private long defaultPeriod = 5 * 60; // Run every five minutes
		
		public TopicDetectionTask(BlockingQueue<Item> queue) {
			this.queue = queue;
		}
		
		public TopicDetectionTask(BlockingQueue<Item> queue, int period) {
			this.queue = queue;
			defaultPeriod = period;
		}
		
		public void run() {
			while(true) {
				try {
					Thread.sleep(defaultPeriod * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
					break;
				}
				
				List<Item> items = new ArrayList<Item>();
				queue.drainTo(items, 1500);
				
				if(items.isEmpty()) {
					_logger.info("Queue of items is empty! ");
					continue;
				}
				else {
					_logger.info("Start topic detection for " + items.size() + " items");
					_logger.info(queue.size() + " tuples remain to queue.");
				}
				
				try {
					
					_logger.info("Run topic detection...");
					
					_logger.info("Done!");

				} catch (Exception e) {
					e.printStackTrace();
					_logger.error(e);
				}	
			}	
		}
	}
}
