package eu.socialsensor.focused.crawler.bolts.metrics;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import eu.socialsensor.framework.common.domain.MediaItem;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class MediaCounterBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private Map<String, Integer> domains = new HashMap<String, Integer>();
	private Map<String, Integer> tags = new HashMap<String, Integer>();
	private Map<String, Integer> contributors = new HashMap<String, Integer>();

	private String mongoHostName;

	private String mongodbName;
	

	public MediaCounterBolt(String mongoHostName, String mongodbName) {
		this.mongoHostName = mongoHostName;
		this.mongodbName = mongodbName;
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		try {
			MongoClient client = new MongoClient(mongoHostName);
			DB db  = client.getDB(mongodbName);
			
			Runnable updater = new MediaCounterUpdater(db);
			Thread thread = new Thread(updater);
			
			thread.start();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	public void execute(Tuple input) {
		MediaItem mediaItem = (MediaItem) input.getValueByField("MediaItem");
		if(mediaItem == null)
			return;
		

		try {
			URL mediaItemUrl = new URL(mediaItem.getUrl());
			String domain = mediaItemUrl.getHost();
			synchronized(domains) {
				Integer count = domains.get(domain);
				if(count == null)
					count = 0;
				domains.put(domain, ++count);
			}
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		
		

		String[] miTags = mediaItem.getTags();
		if(miTags != null && miTags.length>0) {
			synchronized(tags) {
				for(String tag : miTags) {
					Integer count = tags.get(tag);
					if(count == null)
						count = 0;
					tags.put(tag, ++count);
				}
			}
		}

		String contributor = mediaItem.getUserId();
		synchronized(contributors) {
			Integer count = contributors.get(contributor);
			if(count == null)
				count = 0;
			contributors.put(contributor, ++count);
		}
		
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	public class MediaCounterUpdater implements Runnable {

		private DBCollection _tagsCollection, _domainsCollection, _contributorsCollection;

		public MediaCounterUpdater(DB db) {
			_tagsCollection = db.getCollection("tags");
			_domainsCollection = db.getCollection("domains");
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
				
				synchronized(tags) {
					for(Entry<String, Integer> tagEntry : tags.entrySet()) {
						DBObject q = new BasicDBObject("tag", tagEntry.getKey());
						DBObject o = new BasicDBObject("$inc", new BasicDBObject("count", tagEntry.getValue()));
						_tagsCollection.update(q, o);
					}
					tags.clear();
				}
				
				synchronized(domains) {
					for(Entry<String, Integer> domainEntry : domains.entrySet()) {
						DBObject q = new BasicDBObject("domain", domainEntry.getKey());
						DBObject o = new BasicDBObject("$inc", new BasicDBObject("count", domainEntry.getValue()));
						_domainsCollection.update(q, o);
					}
					domains.clear();
				}
				
				synchronized(contributors) {
					for(Entry<String, Integer> contributorEntry : contributors.entrySet()) {
						DBObject q = new BasicDBObject("contributor", contributorEntry.getKey());
						DBObject o = new BasicDBObject("$inc", new BasicDBObject("count", contributorEntry.getValue()));
						_contributorsCollection.update(q, o);
					}
					contributors.clear();
				}
			}
		}
		
	}
}
