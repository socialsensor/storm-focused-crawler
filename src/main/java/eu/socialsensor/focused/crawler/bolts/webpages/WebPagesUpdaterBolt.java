package eu.socialsensor.focused.crawler.bolts.webpages;

import java.util.Map;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.client.dao.WebPageDAO;
import eu.socialsensor.framework.client.dao.impl.WebPageDAOImpl;
import eu.socialsensor.framework.client.mongo.UpdateItem;
import eu.socialsensor.framework.common.domain.WebPage;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class WebPagesUpdaterBolt extends BaseRichBolt {

    /**
	 * 
	 */
	private static final long serialVersionUID = -2548434425109192911L;
	
	private Logger logger;
	
	private String mongodbHostname;
	private String webPagesDB;
	private String webPagesCollection;
	
	private WebPageDAO _webPageDAO = null;
	
	public WebPagesUpdaterBolt(String mongodbHostname, String webPagesDB, String webPagesCollection) {
		this.mongodbHostname = mongodbHostname;
		
		this.webPagesDB = webPagesDB;
		this.webPagesCollection = webPagesCollection;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		
		logger = Logger.getLogger(WebPagesUpdaterBolt.class);
		try {
			_webPageDAO = new WebPageDAOImpl(mongodbHostname, webPagesDB, webPagesCollection);
		} catch (Exception e) {
			logger.error(e);
		}
		
	}

	public void execute(Tuple tuple) {
		try {
			WebPage webPage = (WebPage) tuple.getValueByField("WebPage");
		
			if(webPage == null || _webPageDAO == null)
				return;
				
			if(_webPageDAO.exists(webPage.getUrl())) {
				
				// Update existing web page
				UpdateItem o = new UpdateItem();
				o.setField("status", webPage.getStatus());
				o.setField("isArticle", webPage.isArticle());
				o.setField("text", webPage.getText());
				o.setField("domain", webPage.getDomain());
				o.setField("expandedUrl", webPage.getExpandedUrl());
				
				_webPageDAO.updateWebPage(webPage.getUrl(), o);
			}
			else {
				// Insert new web page (this should never happen)
				_webPageDAO.addWebPage(webPage);
			}
			
			
		}
		catch(Exception ex) {
			logger.error(ex);
		}
		
	}
 
}