package eu.socialsensor.focused.crawler.items;

import static backtype.storm.utils.Utils.tuple;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.factories.ItemFactory;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

public class ItemDeserializationBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Logger logger;
	
	private OutputCollector _collector;
	private Queue<String> _queue;

	private String inputField;

	public ItemDeserializationBolt(String inputField) {
		this.inputField = inputField;
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector = collector;
		this._queue = new LinkedBlockingQueue<String>();
		
		logger = Logger.getLogger(ItemDeserializationBolt.class);
		
		Thread[] threads = new Thread[4];
		for(int i=0; i<4; i++) {
			threads[i] = new Thread(new DeserializerThread(_queue));
			threads[i].start();
		}
	}

	public void execute(Tuple input) {
		try {
			String json = input.getStringByField(inputField);
			if(json != null) {
				synchronized(_queue) {
					_queue.offer(json);
				}
			}
		} catch(Exception e) {
				logger.error("Exception: "+e.getMessage());
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Item"));
	}

	class DeserializerThread extends Thread {

		Queue<String> queue;
		public DeserializerThread(Queue<String> queue) {	
			this.queue = queue;
		}

		public void run() {
			while(true) {
				try {
					String json = null;
					synchronized(_queue) {
						json = queue.poll();
					}
					
					if(json == null) {
						Utils.sleep(500);
					}
					else {
						Item item = ItemFactory.create(json);
						_collector.emit(tuple(item));
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
