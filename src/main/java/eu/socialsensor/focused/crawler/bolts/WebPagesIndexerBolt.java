package eu.socialsensor.focused.crawler.bolts;

import java.util.Map;

import eu.socialsensor.framework.client.dao.WebPageDAO;
import eu.socialsensor.framework.client.dao.impl.WebPageDAOImpl;
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
	private String _indexService;
	private String _mongoHost;
	private String _mongoDb;
	private String _mongoCollection;
	
	private SolrWebPageHandler solrWebPageHandler;
	private WebPageDAO webPageDAO;
	
	public WebPagesIndexerBolt(String indexService, String mongoHost, String mongoDb, String mongoCollection) {
		this._indexService = indexService;
		
		_mongoHost = mongoHost;
		_mongoDb = mongoDb;
		_mongoCollection = mongoCollection;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		solrWebPageHandler = SolrWebPageHandler.getInstance(_indexService);
		webPageDAO = new WebPageDAOImpl(_mongoHost, _mongoDb, _mongoCollection);
	}

	public void execute(Tuple tuple) {
		
		try {
			String url = tuple.getStringByField("url");
			WebPage webPage = webPageDAO.getWebPage(url);
			
			if(webPage != null) {
				solrWebPageHandler.insertWebPage(webPage);
			}
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
		
	}

}