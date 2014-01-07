package eu.socialsensor.focused.crawler.bolts;

import java.util.Map;

import eu.socialsensor.framework.client.search.solr.SolrWebPageHandler;
import eu.socialsensor.framework.common.domain.WebPage;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;


public class WebPagesIndexerBolt extends BaseRichBolt {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7500656732029697927L;
	private String hostname;
	private String service;
	private String collection;

	private SolrWebPageHandler solrWebPageHandler;
	
	public WebPagesIndexerBolt(String hostname, String service, String collection) {
		this.hostname = hostname;
		this.service = service;
		this.collection = collection;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		solrWebPageHandler = SolrWebPageHandler.getInstance(hostname+"/"+service+"/"+collection);
	}

	public void execute(Tuple tuple) {
		
		try {
			WebPage webPage = (WebPage) tuple.getValueByField("webPage");
			solrWebPageHandler.insertWebPage(webPage);
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
		
	}
 
}