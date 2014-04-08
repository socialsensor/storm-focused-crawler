package eu.socialsensor.focused.crawler.bolts.media;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import eu.socialsensor.framework.client.search.visual.JsonResultSet;
import eu.socialsensor.framework.client.search.visual.JsonResultSet.JsonResult;
import eu.socialsensor.framework.client.search.visual.VisualIndexHandler;

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
	
	private String mongoHost;
	private String mongoDbName;
	private String mongoCollectionName;
	private String clustersCollectionName;
	
	private MongoClient _mongo;
	private DB _database;
	private DBCollection _collection;
	private DBCollection _clustersCollection;

	private Queue<String> _mQ = new LinkedList<String>();

	private VisualIndexHandler _visualIndex;

	private String webServiceHost;
	private String indexCollection;

	public MediaUpdaterBolt(String mongoHost, String mongoDbName, String mongoCollectionName, String clustersCollectionName,
			String webServiceHost, String indexCollection) {
		this.mongoHost = mongoHost;
		this.mongoDbName = mongoDbName;
		this.mongoCollectionName = mongoCollectionName;
		this.clustersCollectionName = clustersCollectionName;
		
		this.webServiceHost = webServiceHost; 
		this.indexCollection = indexCollection;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		
		try {
			_mongo = new MongoClient(mongoHost);
			_database = _mongo.getDB(mongoDbName);
			_collection = _database.getCollection(mongoCollectionName);
			_clustersCollection = _database.getCollection(clustersCollectionName);
			
			_visualIndex = new VisualIndexHandler(webServiceHost, indexCollection);
			
			Thread thread = new Thread(new Clusterer(_mQ, _visualIndex));
			thread.start();
			
		} catch (Exception e) {
			
		}
		
	}

	public void execute(Tuple tuple) {
		String id = tuple.getStringByField("id");
		boolean indexed = tuple.getBooleanByField("indexed");
		Integer width = tuple.getIntegerByField("width");
		Integer height = tuple.getIntegerByField("height");
	
		if(_collection != null) {
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
			
			_collection.update(q, o, false, true);
			_mQ.offer(id);
		}
	}   
	
	public class Clusterer implements Runnable {

		private Queue<String> queue;
		private double threshold = 0.8;
		private VisualIndexHandler visualIndex;
		
		public Clusterer(Queue<String> queue, VisualIndexHandler visualIndex) {
			this.queue = queue;
			this.visualIndex = visualIndex;
		}
		
		public void run() {
			
			while(true) {
				String id = queue.poll();
				if(id == null) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				else {
					
					JsonResultSet response = visualIndex.getSimilarImages(id, threshold);
					List<JsonResult> results = response.getResults();
					if(results.size()>1) {
						String nearestId = results.get(0).getId();
						if(id.equals(nearestId))
							nearestId = results.get(1).getId();
						System.out.println(id + " -> " +nearestId);
					
						DBObject obj = _collection.findOne(new BasicDBObject("id", nearestId));
						if(obj != null && obj.containsField("clusterId")) {
							
							// Add media item to the same cluster as the nearest neighbor
							String clusterId = (String) obj.get("clusterId");
							
							System.out.println("Add " + id + " in cluster " + clusterId + " Nearest: " + nearestId);
							
							_collection.update(new BasicDBObject("id", id), 
									new BasicDBObject("$set", new BasicDBObject("clusterId", clusterId)));
							
							BasicDBObject cluster = new BasicDBObject("id", clusterId);
							DBObject update = new BasicDBObject("$addToSet", new BasicDBObject("members", id));
							update.put("$inc", new BasicDBObject("count", 1));
							_clustersCollection.update(cluster, update);
						}
						else {
							if(obj == null)
								System.out.println("Error: " + nearestId + " not found!");
							else
								System.out.println("Error: " + nearestId + " not clustered!");
						}
					}
					else {
						// Create new Cluster
						UUID clusterId = UUID.randomUUID();
						
						//System.out.println("Create new cluster" + clusterId + " for " + id);
						
						_collection.update(new BasicDBObject("id", id), 
								new BasicDBObject("$set", new BasicDBObject("clusterId", clusterId.toString())));
						
						BasicDBObject cluster = new BasicDBObject("id", clusterId.toString());
						
						List<String> members = new ArrayList<String>();
						members.add(id);
						cluster.put("members", members);
						cluster.put("count", 1);
						_clustersCollection.insert(cluster);
					}
					
				}
			}
		}
		
	}
	
}