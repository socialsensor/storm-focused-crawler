package eu.socialsensor.focused.crawler.bolts.media;

import java.util.Map;

import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;


public class MediaUpdaterBolt extends BaseRichBolt {

    /**
	 * 
	 */
	private static final long serialVersionUID = -2548434425109192911L;
	
	Logger logger;
	
	private String mongoHost;
	
	private String mediaItemsDbName;
	private String mediaItemsCollectionName;
	
	private DBCollection _mediaItemsCollection;

	public MediaUpdaterBolt(String mongoHost, String mediaItemsDbName, String mediaItemsCollectionName) {
		this.mongoHost = mongoHost;
		this.mediaItemsDbName = mediaItemsDbName;
		this.mediaItemsCollectionName = mediaItemsCollectionName;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		
		logger = Logger.getLogger(MediaUpdaterBolt.class);
		
		try {
			MongoClient mongo = new MongoClient(mongoHost);
			DB _database = mongo.getDB(mediaItemsDbName);
			_mediaItemsCollection = _database.getCollection(mediaItemsCollectionName);
			
		} catch (Exception e) {
			logger.error(e);
		}
		
	}

	public void execute(Tuple tuple) {
		String id = tuple.getStringByField("id");
		boolean indexed = tuple.getBooleanByField("indexed");
		Integer width = tuple.getIntegerByField("width");
		Integer height = tuple.getIntegerByField("height");
	
		if(_mediaItemsCollection != null) {
			DBObject q = new BasicDBObject("id", id);
			
			BasicDBObject f = new BasicDBObject("vIndexed", indexed);
			if(indexed)
				f.put("status", "indexed");
			else
				f.put("status", "failed");
			
			if(width!=null && height!=null && width!=-1 && height!=-1) {
				f.put("height", height);
				f.put("width", width);
			}
			
			DBObject o = new BasicDBObject("$set", f);
			
			_mediaItemsCollection.update(q, o, false, true);

		}
	}   
	
}