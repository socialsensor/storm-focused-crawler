package eu.socialsensor.focused.crawler.bolts.webpages;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.client.search.solr.SolrWebPageHandler;
import eu.socialsensor.framework.common.domain.WebPage;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class TextIndexerBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7500656732029697927L;
	
	private Logger logger;
	
	private String _indexService;
	private SolrWebPageHandler _solrWebPageHandler = null;
	
	private ArrayBlockingQueue<WebPage> _queue;
	
	public TextIndexerBolt(String indexService) {
		this._indexService = indexService;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		logger = Logger.getLogger(TextIndexerBolt.class);
		
		_queue = new ArrayBlockingQueue<WebPage>(10000);
		_solrWebPageHandler = SolrWebPageHandler.getInstance(_indexService);
		
		Thread t = new Thread(new TextIndexer());
		t.start();
	}

	public void execute(Tuple tuple) {
		try {
			WebPage webPage = (WebPage) tuple.getValueByField("WebPage");
			
			if(webPage != null && _solrWebPageHandler != null) {
				_queue.add(webPage);
			}
		}
		catch(Exception ex) {
			logger.error(ex);
		}
	}

	public class TextIndexer implements Runnable {

		public void run() {
			while(true) {
				try {
					// Just wait 10 seconds
					Thread.sleep(10 * 1000);

					List<WebPage> webPages = new ArrayList<WebPage>();
					_queue.drainTo(webPages);
					
					if(webPages.isEmpty()) {
						logger.info("There are no web pages to index. Wait some more time.");
						continue;
					}
					
					boolean inserted = _solrWebPageHandler.insertWebPages(webPages);
					
					if(inserted) {
						logger.info(webPages.size() + " web pages indexed in Solr");
					}
					else {
						logger.error("Indexing in Solr failed for some web pages");
					}
				} catch (Exception e) {
					logger.error(e);
					continue;
				}
			}
		}
		
	}
}