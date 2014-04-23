package eu.socialsensor.focused.crawler.bolts.media;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.client.search.solr.SolrMediaItemHandler;
import eu.socialsensor.framework.common.domain.MediaItem;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;


public class MediaTextIndexerBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7500656732029697927L;
	
	private Logger logger;
	
	private String hostname;
	private String service;
	private String collection;

	private SolrMediaItemHandler solrMediaHandler;

	private ArrayBlockingQueue<MediaItem> queue;
	
	public MediaTextIndexerBolt(String hostname, String service, String collection) {
		this.hostname = hostname;
		this.service = service;
		this.collection = collection;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		logger = Logger.getLogger(MediaTextIndexerBolt.class);
		
		queue = new ArrayBlockingQueue<MediaItem>(5000);
		try {
			solrMediaHandler = SolrMediaItemHandler.getInstance(hostname+"/"+service+"/"+collection);
		} catch (Exception e) {
			e.printStackTrace();
			solrMediaHandler = null;
			logger.error(e);
		}
		
		Thread t = new Thread(new TextIndexer());
		t.start();
	}

	public void execute(Tuple tuple) {
		
		try {
			MediaItem mediaItem = (MediaItem) tuple.getValueByField("MediaItem");
		
			if(mediaItem == null || solrMediaHandler == null)
				return;
			
			queue.add(mediaItem);
		}
		catch(Exception ex) {
			ex.printStackTrace();
			logger.error(ex);
		}
		
	}
 
	public class TextIndexer implements Runnable {

		public void run() {
			while(true) {
				try {
					Thread.sleep(60 * 1000);

					List<MediaItem> mItems = new ArrayList<MediaItem>();
					queue.drainTo(mItems);
					boolean inserted = solrMediaHandler.insertMediaItems(mItems);
					
					if(inserted) {
						logger.info(mItems.size() + " media items indexed in Solr");
					}
					else {
						logger.error("Indexing in Solr failed for MediaItem");
					}
				} catch (Exception e) {
					logger.error(e);
					continue;
				}
			}
		}
		
	}
}