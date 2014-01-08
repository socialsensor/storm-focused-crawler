package eu.socialsensor.focused.crawler.bolts;

import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

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


public class UpdaterBolt extends BaseRichBolt {

    /**
	 * 
	 */
	private static final long serialVersionUID = -2548434425109192911L;
	private String mongoHost;
	private String mongoDbName;
	private String webpagesCollectionName;
	private String mediaCollectionName;
	
	private String redisHost;
	private String redisChannel;
	
	private MongoClient _mongo;
	private DB _database;
	private DBCollection _pagesCollection, _mediaCollection;
	
	private Jedis _publisherJedis;
	

	public UpdaterBolt(String mongoHost, String mongoDbName, String webpagesCollectionName, String mediaCollectionName, 
			String redisHost, String redisChannel) {
		this.mongoHost = mongoHost;
		this.mongoDbName = mongoDbName;
		this.webpagesCollectionName = webpagesCollectionName;
		this.mediaCollectionName = mediaCollectionName;
		
		this.redisHost = redisHost;
		this.redisChannel = redisChannel;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		try {
			_mongo = new MongoClient(mongoHost);
			_database = _mongo.getDB(mongoDbName);
			_pagesCollection = _database.getCollection(webpagesCollectionName);
			_mediaCollection = _database.getCollection(mediaCollectionName);
			
			JedisPoolConfig poolConfig = new JedisPoolConfig();
	        JedisPool jedisPool = new JedisPool(poolConfig, redisHost, 6379, 0);
			
	        this._publisherJedis = jedisPool.getResource();
			
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
			
				if(_mediaCollection.findOne(new BasicDBObject("id", mediaItem.getId()))==null) {
					DBObject doc = (DBObject) JSON.parse(mediaItem.toJSONString());
					_mediaCollection.insert(doc);
					_publisherJedis.publish(redisChannel, mediaItem.toJSONString());
				}
				DBObject o = new BasicDBObject("$set", new BasicDBObject("status", "proccessed"));
				_pagesCollection.update(q, o);
			}
			else if(type.equals("article")) {
				Article article = (Article) tuple.getValueByField("content");

				List<MediaItem> mediaItems = article.getMediaItems();
				
				DBObject fields = new BasicDBObject("title", article.getTitle());
				fields.put("text", article.getText());
				fields.put("quality", article.isLowQuality()?"low":"good");
				fields.put("isArticle", true);
				fields.put("status", "proccessed");
				fields.put("media", mediaItems.size());
				
				DBObject o = new BasicDBObject("$set", fields);
				
				_pagesCollection.update(q, o);
				
				for(MediaItem mediaItem : mediaItems) {
					try {
						if(_mediaCollection.findOne(new BasicDBObject("id", mediaItem.getId()))==null) {
							DBObject doc = (DBObject) JSON.parse(mediaItem.toJSONString());
							_mediaCollection.insert(doc);
							_publisherJedis.publish(redisChannel, mediaItem.toJSONString());
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