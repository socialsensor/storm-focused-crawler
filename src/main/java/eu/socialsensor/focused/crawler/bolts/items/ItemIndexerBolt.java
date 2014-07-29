package eu.socialsensor.focused.crawler.bolts.items;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.client.search.solr.SolrItemHandler;
import eu.socialsensor.framework.common.domain.Item;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ItemIndexerBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7500656732029697927L;
	
	private Logger _logger;
	private String _service;
	private SolrItemHandler _solrItemHandler;

	private ArrayBlockingQueue<Item> queue;
	
	public ItemIndexerBolt(String service) {
		this._service = service;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("Item"));
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		_logger = Logger.getLogger(ItemIndexerBolt.class);
		
		queue = new ArrayBlockingQueue<Item>(5000);
		try {
			_solrItemHandler = SolrItemHandler.getInstance(_service);
		} catch (Exception e) {
			e.printStackTrace();
			_solrItemHandler = null;
			_logger.error(e);
		}
		
		Thread t = new Thread(new TextIndexer());
		t.start();
	}

	public void execute(Tuple tuple) {	
		try {
			Item mediaItem = (Item) tuple.getValueByField("Item");
		
			if(mediaItem == null || _solrItemHandler == null)
				return;
			
			queue.add(mediaItem);
		}
		catch(Exception ex) {
			ex.printStackTrace();
			_logger.error(ex);
		}		
	}
 
	public class TextIndexer implements Runnable {
		public void run() {
			while(true) {
				try {
					Thread.sleep(60 * 1000);

					List<Item> items = new ArrayList<Item>();
					synchronized(queue) {
						queue.drainTo(items);
					}
					
					boolean inserted = _solrItemHandler.insertItems(items);
					
					if(inserted) {
						_logger.info(items.size() + " items indexed in Solr");
					}
					else {
						_logger.error("Indexing in Solr failed for Items");
					}
				} catch (Exception e) {
					_logger.error(e);
					continue;
				}
			}
		}
		
	}
}