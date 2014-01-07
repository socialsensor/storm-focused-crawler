package eu.socialsensor.focused.crawler.bolts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

import eu.socialsensor.focused.crawler.models.Article;
import eu.socialsensor.framework.common.domain.MediaItem;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;


public class PrinterBolt extends BaseRichBolt {

    /**
	 * 
	 */
	private static final long serialVersionUID = -2548434425109192911L;
	private String mongoHost;
	private String mongoDbName;
	private String collectionName;
	
	private MongoClient _mongo;
	private DB _database;
	private DBCollection _collection;
	

	public PrinterBolt(String mongoHost, String mongoDbName, String collectionName) {
		this.mongoHost = mongoHost;
		this.mongoDbName = mongoDbName;
		this.collectionName = collectionName;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		try {
			_mongo = new MongoClient(mongoHost);
			_database = _mongo.getDB(mongoDbName);
			_collection = _database.getCollection(collectionName);
		} catch (Exception e) {
			
		}
		
	}

	public void execute(Tuple tuple) {
		
		try {
			String url = tuple.getStringByField("url");
			String type = tuple.getStringByField("type");
		
			DBObject q = new BasicDBObject("url", url);
		
			if(type.equals("article")) {
				Article article = (Article) tuple.getValueByField("content");

				List<MediaItem> mediaItems = article.getMediaItems();
				
				DBObject obj = new BasicDBObject("url", url);
				obj.put("text", article.getText());
				obj.put("title", article.getTitle());
				obj.put("quality", article.isLowQuality()?"LOW":"GOOD");
				
				List<DBObject> media = new ArrayList<DBObject>();
				for(MediaItem mi : mediaItems) {
					String json = mi.toJSONString();
					DBObject object = (DBObject) JSON.parse(json);
					media.add(object);
				}
				obj.put("media", media);
				
				DBObject o = new BasicDBObject("$set", obj);
				_collection.update(q, o, true, false);
			}
			else {
				String msg = tuple.getStringByField("exception");
				DBObject obj = new BasicDBObject("status", "failed");
				obj.put("error", msg);
				
				DBObject o = new BasicDBObject("$set", obj);
				_collection.update(q, o, true, false);
			}
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
		
	}   
}