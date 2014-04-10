package eu.socialsensor.focused.crawler.bolts.webpages;

import static backtype.storm.utils.Utils.tuple;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

import eu.socialsensor.focused.crawler.models.Article;
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
	
	private Logger logger = Logger.getLogger(UpdaterBolt.class);
	
	private String mongodbHostname;
	
	private String mediaItemsDB;
	private String mediaItemsCollection;
	
	private String webPagesDB;
	private String webPagesCollection;
	
	
	private MongoClient _mongo;
	private DB _mediadatabase, _webpagesdatabase;
	private DBCollection _pagesCollection, _mediaCollection;
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
		
		try {
			_collector = collector;
			_mongo = new MongoClient(mongodbHostname);
			
			_mediadatabase = _mongo.getDB(mediaItemsDB);
			_mediaCollection = _mediadatabase.getCollection(mediaItemsCollection);
			
			_webpagesdatabase = _mongo.getDB(webPagesDB);
			_pagesCollection = _webpagesdatabase.getCollection(webPagesCollection);
			
			
		} catch (Exception e) {
			logger.error(e);
		}
		
	}

	public void execute(Tuple tuple) {
		
		try {
			String url = tuple.getStringByField("url");
			String type = tuple.getStringByField("type");
		
			if(url == null || type == null)
				return;
			
			DBObject q = new BasicDBObject("url", url);
		
			if(type.equals("media")) {
				MediaItem mediaItem = (MediaItem) tuple.getValueByField("content");
			
				if(_mediaCollection.count(new BasicDBObject("id", mediaItem.getId()))==0) {
					DBObject doc = (DBObject) JSON.parse(mediaItem.toJSONString());
					_mediaCollection.insert(doc);
				}
				DBObject o = new BasicDBObject("$set", new BasicDBObject("status", "processed"));
				_pagesCollection.update(q, o);
			}
			else if(type.equals("article")) {
				
				Article article = (Article) tuple.getValueByField("content");

				List<MediaItem> mediaItems = article.getMediaItems();
				
				DBObject fields = new BasicDBObject("title", article.getTitle());
				fields.put("text", article.getText());
				fields.put("quality", article.isLowQuality() ? "low" : "good");
				fields.put("isArticle", true);
				fields.put("status", "proccessed");
				fields.put("media", mediaItems.size());
				
				DBObject o = new BasicDBObject("$set", fields);
				
				WriteResult result = _pagesCollection.update(q, o);
				if(result.getN()>0) {
					_collector.emit(tuple(url));
				}
				
				for(MediaItem mediaItem : mediaItems) {
					try {
						if(_mediaCollection.count(new BasicDBObject("id", mediaItem.getId()))==0) {
							DBObject doc = (DBObject) JSON.parse(mediaItem.toJSONString());
							_mediaCollection.insert(doc);
						}
					}
					catch(Exception e) {
						logger.error(e);
						continue;
					}
				}	
			}
			else {
				DBObject o = new BasicDBObject("$set", new BasicDBObject("status", "failed"));
				_pagesCollection.update(q, o);
			}
		}
		catch(Exception ex) {
			logger.error(ex);
		}
		
	}
 
}