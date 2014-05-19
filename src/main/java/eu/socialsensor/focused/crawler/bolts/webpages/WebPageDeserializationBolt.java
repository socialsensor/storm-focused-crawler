package eu.socialsensor.focused.crawler.bolts.webpages;

import static backtype.storm.utils.Utils.tuple;

import java.util.Map;
import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.WebPage;
import eu.socialsensor.framework.common.factories.ItemFactory;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class WebPageDeserializationBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Logger _logger;
	
	private OutputCollector _collector;
	//private Queue<WebPage> _queue;

	private String inputField;

	public WebPageDeserializationBolt(String inputField) {
		this.inputField = inputField;
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector = collector;
		//this._queue = new LinkedBlockingQueue<WebPage>();
		
		_logger = Logger.getLogger(WebPageDeserializationBolt.class);
		
		/*
		Thread[] threads = new Thread[4];
		for(int i=0; i<4; i++) {
			threads[i] = new Thread(new DeserializerThread(_queue));
			threads[i].start();
		}
		*/
	}

	public void execute(Tuple input) {
		try {
			String json = input.getStringByField(inputField);
			WebPage webPage = ItemFactory.createWebPage(json);
			if(webPage != null) {
				_collector.emit(tuple(webPage));
			}
		} catch(Exception e) {
			_logger.error("Exception: "+e.getMessage());
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("webpages"));
	}

	/*
	class DeserializerThread extends Thread {

		Queue<WebPage> queue;
		
		public DeserializerThread(Queue<WebPage> queue) {	
			this.queue = queue;
		}

		public void run() {
			while(true) {
				try {
					WebPage webPage = null;
					synchronized(_queue) {
						webPage = queue.poll();
					}
					
					if(webPage == null) {
						Utils.sleep(500);
					}
					else {
						_collector.emit(tuple(webPage));
					}
				}
				catch(Exception e) {
					Utils.sleep(500);
					continue;
				}
			}
		};
	}
	*/
}
