package eu.socialsensor.focused.crawler.bolts;

import static backtype.storm.utils.Utils.tuple;

import java.util.List;
import java.util.Map;

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
	private String mongoHost;
	private String mongoDbName;
	private String webpagesCollectionName;
	private String mediaCollectionName;
	
	private MongoClient _mongo;
	private DB _database;
	private DBCollection _pagesCollection, _mediaCollection;
	private OutputCollector _collector;

	public UpdaterBolt(String mongoHost, String mongoDbName, String webpagesCollectionName, String mediaCollectionName) {
		this.mongoHost = mongoHost;
		this.mongoDbName = mongoDbName;
		this.webpagesCollectionName = webpagesCollectionName;
		this.mediaCollectionName = mediaCollectionName;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("url"));
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		
		try {
			_collector = collector;
			_mongo = new MongoClient(mongoHost);
			_database = _mongo.getDB(mongoDbName);
			_pagesCollection = _database.getCollection(webpagesCollectionName);
			_mediaCollection = _database.getCollection(mediaCollectionName);
			
		} catch (Exception e) {
			
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
			ex.printStackTrace();
		}
		
	}
 
}