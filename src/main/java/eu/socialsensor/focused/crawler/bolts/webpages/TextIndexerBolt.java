package eu.socialsensor.focused.crawler.bolts.webpages;

import java.util.Map;

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
	
	public TextIndexerBolt(String indexService) {
		this._indexService = indexService;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		logger = Logger.getLogger(TextIndexerBolt.class);
		solrWebPageHandler = SolrWebPageHandler.getInstance(_indexService);
	}

	public void execute(Tuple tuple) {
		try {
			WebPage webPage = (WebPage) tuple.getValueByField("WebPage");
			if(webPage != null && solrWebPageHandler != null) {
				solrWebPageHandler.insertWebPage(webPage);
			}
		}
		catch(Exception ex) {
			logger.error(ex);
		}
	}

}