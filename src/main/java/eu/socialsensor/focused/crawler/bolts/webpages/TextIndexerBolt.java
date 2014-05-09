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
	private SolrWebPageHandler solrWebPageHandler = null;
	
	private ArrayBlockingQueue<WebPage> queue;
	
	public TextIndexerBolt(String indexService) {
		this._indexService = indexService;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		logger = Logger.getLogger(TextIndexerBolt.class);
		
		queue = new ArrayBlockingQueue<WebPage>(10000);
		solrWebPageHandler = SolrWebPageHandler.getInstance(_indexService);
		
		Thread t = new Thread(new TextIndexer());
		t.start();
	}

	public void execute(Tuple tuple) {
		try {
			WebPage webPage = (WebPage) tuple.getValueByField("WebPage");
			
			if(webPage != null && solrWebPageHandler != null) {
				queue.add(webPage);
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
					Thread.sleep(60 * 1000);

					List<WebPage> webPages = new ArrayList<WebPage>();
					queue.drainTo(webPages);
					
					boolean inserted = solrWebPageHandler.insertWebPages(webPages);
					
					if(inserted) {
						logger.info(webPages.size() + " web pages indexed in Solr");
					}
					else {
						logger.error("Indexing in Solr failed for web pages");
					}
				} catch (Exception e) {
					logger.error(e);
					continue;
				}
			}
		}
		
	}
}