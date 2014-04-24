package eu.socialsensor.focused.crawler.bolts.metrics;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import eu.socialsensor.framework.common.domain.Item;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class ItemsCounterBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private Map<String, Integer> tags = new HashMap<String, Integer>();
	private Map<String, Integer> contributors = new HashMap<String, Integer>();

	private String mongoHostName;
	private String mongodbName;

	private Logger logger;

	public ItemsCounterBolt(String mongoHostName, String mongodbName) {
		this.mongoHostName = mongoHostName;
		this.mongodbName = mongodbName;
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		logger = Logger.getLogger(ItemsCounterBolt.class);
		
		try {
			MongoClient client = new MongoClient(mongoHostName);
			DB db  = client.getDB(mongodbName);
			
			Runnable updater = new ItemCounterUpdater(db);
			Thread thread = new Thread(updater);
			
			thread.start();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	public void execute(Tuple input) {
		Item item = (Item) input.getValueByField("Item");
		if(item == null)
			return;
		
		String[] itemTags = item.getTags();
		if(itemTags != null && itemTags.length>0) {
			synchronized(tags) {
				for(String tag : itemTags) {
					Integer count = tags.get(tag);
					if(count == null)
						count = 0;
					tags.put(tag, ++count);
				}
			}
		}

		String contributor = item.getUserId();
		synchronized(contributors) {
			Integer count = contributors.get(contributor);
			if(count == null)
				count = 0;
			contributors.put(contributor, ++count);
		}
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	public class ItemCounterUpdater implements Runnable {

		private DBCollection _tagsCollection, _contributorsCollection;

		public ItemCounterUpdater(DB db) {
			_tagsCollection = db.getCollection("tags");
			_contributorsCollection = db.getCollection("contributor");
		}
		
		public void run() {
			while(true) {
				try {
					Thread.sleep(10 * 60 * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
					continue;
				}
				
				logger.info("============ Update Counters ============= ");
				synchronized(tags) {
					logger.info(tags.size() + " tags to be updated");
					for(Entry<String, Integer> tagEntry : tags.entrySet()) {
						DBObject q = new BasicDBObject("tag", tagEntry.getKey());
						DBObject o = new BasicDBObject("$inc", new BasicDBObject("count", tagEntry.getValue()));
						_tagsCollection.update(q, o, true, false);
					}
					tags.clear();
				}
				
				synchronized(contributors) {
					logger.info(contributors.size() + " contributors to be updated");
					for(Entry<String, Integer> contributorEntry : contributors.entrySet()) {
						DBObject q = new BasicDBObject("contributor", contributorEntry.getKey());
						DBObject o = new BasicDBObject("$inc", new BasicDBObject("count", contributorEntry.getValue()));
						_contributorsCollection.update(q, o, true, false);
					}
					contributors.clear();
				}
			}
		}
		
	}
}
