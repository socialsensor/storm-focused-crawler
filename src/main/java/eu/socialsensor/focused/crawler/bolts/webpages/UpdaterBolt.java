package eu.socialsensor.focused.crawler.bolts.webpages;

import static backtype.storm.utils.Utils.tuple;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import eu.socialsensor.focused.crawler.models.Article;
import eu.socialsensor.framework.client.dao.MediaItemDAO;
import eu.socialsensor.framework.client.dao.WebPageDAO;
import eu.socialsensor.framework.client.dao.impl.MediaItemDAOImpl;
import eu.socialsensor.framework.client.dao.impl.WebPageDAOImpl;
import eu.socialsensor.framework.client.mongo.UpdateItem;
import eu.socialsensor.framework.common.domain.MediaItem;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class UpdaterBolt extends BaseRichBolt {

    /**
	 * 
	 */
	private static final long serialVersionUID = -2548434425109192911L;
	
	private Logger logger;
	
	private String mongodbHostname;
	
	private String mediaItemsDB;
	private String mediaItemsCollection;
	
	private String webPagesDB;
	private String webPagesCollection;
	
	private MediaItemDAO _mediaItemDAO;
	private WebPageDAO _webPageDAO;
	
	private OutputCollector _collector;

	
	public UpdaterBolt(String mongodbHostname, String mediaItemsDB, String mediaItemsCollection, String webPagesDB, String webPagesCollection) {
		this.mongodbHostname = mongodbHostname;
		
		this.mediaItemsDB = mediaItemsDB;
		this.mediaItemsCollection = mediaItemsCollection;
		
		this.webPagesDB = webPagesDB;
		this.webPagesCollection = webPagesCollection;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("url"));
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		
		logger = Logger.getLogger(UpdaterBolt.class);
		
		try {
			_collector = collector;
			
			_mediaItemDAO = new MediaItemDAOImpl(mongodbHostname, mediaItemsDB, mediaItemsCollection);
			_webPageDAO = new WebPageDAOImpl(mongodbHostname, webPagesDB, webPagesCollection);
			
		} catch (Exception e) {
			logger.error(e);
		}
		
	}

	public void execute(Tuple tuple) {
		
		try {
			String url = tuple.getStringByField("url");
			String expandedUrl = tuple.getStringByField("expandedUrl");
			String domain = tuple.getStringByField("domain");
			
			String type = tuple.getStringByField("type");
		
			if(url == null || type == null)
				return;
		
			if(type.equals("media")) {
				MediaItem mediaItem = (MediaItem) tuple.getValueByField("content");
			
				if(!_mediaItemDAO.exists(mediaItem.getId())) {
					_mediaItemDAO.addMediaItem(mediaItem);
					_collector.emit(tuple(mediaItem));
				}

				UpdateItem o = new UpdateItem();
				o.setField("status", "processed");
				o.setField("domain", domain);
				o.setField("expandedUrl", expandedUrl);
				
				_webPageDAO.updateWebPage(url, o);
			}
			else if(type.equals("article")) {
				
				Article article = (Article) tuple.getValueByField("content");

				List<MediaItem> mediaItems = article.getMediaItems();
				
				UpdateItem o = new UpdateItem();
				o.setField("status", "processed");
				o.setField("isArticle", true);
				o.setField("media", mediaItems.size());
				o.setField("quality", article.isLowQuality() ? "low" : "good");
				o.setField("text", article.getText());
				o.setField("domain", domain);
				o.setField("expandedUrl", expandedUrl);
				
				_webPageDAO.updateWebPage(url, o);

				for(MediaItem mediaItem : mediaItems) {
					try {
						if(_mediaItemDAO.exists(mediaItem.getId())) {
							_mediaItemDAO.addMediaItem(mediaItem);
							_collector.emit(tuple(mediaItem));
						}
					}
					catch(Exception e) {
						logger.error(e);
						continue;
					}
				}	
			}
			else {
				UpdateItem o = new UpdateItem();
				o.setField("status", "failed");
				o.setField("domain", domain);
				o.setField("expandedUrl", expandedUrl);
				
				_webPageDAO.updateWebPage(url, o);
			}
		}
		catch(Exception ex) {
			logger.error(ex);
		}
		
	}
 
}