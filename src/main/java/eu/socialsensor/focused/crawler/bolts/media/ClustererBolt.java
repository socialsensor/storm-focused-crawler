package eu.socialsensor.focused.crawler.bolts.media;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import eu.socialsensor.framework.client.dao.MediaClusterDAO;
import eu.socialsensor.framework.client.dao.impl.MediaClusterDAOImpl;
import eu.socialsensor.framework.client.search.visual.JsonResultSet;
import eu.socialsensor.framework.client.search.visual.JsonResultSet.JsonResult;
import eu.socialsensor.framework.client.search.visual.VisualIndexHandler;
import eu.socialsensor.framework.common.domain.MediaCluster;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class ClustererBolt extends BaseRichBolt {

    /**
	 * 
	 */
	private static final long serialVersionUID = -2548434425109192911L;
	
	Logger logger;
	
	private String mongoHost;
	
	private String mediaItemsDbName;
	private String mediaItemsCollectionName;
	
	private String clustersDbName;
	private String clustersCollectionName;
	
	private DBCollection _mediaItemsCollection;
	
	private MediaClusterDAO mediaClusterDAO;

	private Queue<Pair<?, ?>> _mQ = new LinkedList<Pair<?, ?>>();

	private VisualIndexHandler _visualIndex;

	private String indexHostname;
	private String indexCollection;

	private double threshold = 0.75;
	
	public ClustererBolt(String mongoHost, String mediaItemsDbName, String mediaItemsCollectionName, String clustersDbName, 
			String clustersCollectionName, String indexHostname, String indexCollection) {
		this.mongoHost = mongoHost;
		this.mediaItemsDbName = mediaItemsDbName;
		this.mediaItemsCollectionName = mediaItemsCollectionName;
		this.clustersDbName = clustersDbName;
		this.clustersCollectionName = clustersCollectionName;
		
		this.indexHostname = indexHostname; 
		this.indexCollection = indexCollection;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		
		logger = Logger.getLogger(ClustererBolt.class);
		
		try {
			MongoClient mongo = new MongoClient(mongoHost);
			DB _database = mongo.getDB(mediaItemsDbName);
			_mediaItemsCollection = _database.getCollection(mediaItemsCollectionName);
			
			mediaClusterDAO = new MediaClusterDAOImpl(mongoHost, clustersDbName, clustersCollectionName);
			
			_visualIndex = new VisualIndexHandler(indexHostname, indexCollection);
			
			Thread thread = new Thread(new Clusterer(_mQ));
			thread.start();
			
		} catch (Exception e) {
			logger.error(e);
		}
		
	}

	public void execute(Tuple tuple) {
		String id = tuple.getStringByField("id");

		JsonResultSet response = _visualIndex.getSimilarImages(id, threshold);
		List<JsonResult> results = response.getResults();
		if(results.size()>1) {
			String nearestId = results.get(0).getId();
			if(id.equals(nearestId))
				nearestId = results.get(1).getId();
			
			_mQ.offer(Pair.of(id, nearestId));
		}
		else {
			_mQ.offer(Pair.of(id, null));
		}
			
		
		
	}   
	
	public class Clusterer implements Runnable {

		private Queue<Pair<?, ?>> queue;
		
		public Clusterer(Queue<Pair<?, ?>> queue) {
			this.queue = queue;
		}
		
		public void run() {
			
			while(true) {
				Pair<?, ?> pair = queue.poll();
				if(pair == null) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						logger.error(e);
					}
				}
				else {
					String id = (String) pair.getLeft();
					Object nearestId = pair.getRight();
					
					if(nearestId != null) {
					
						DBObject obj = _mediaItemsCollection.findOne(new BasicDBObject("id", nearestId));
						if(obj != null && obj.containsField("clusterId")) {
							
							// Add media item to the same cluster as the nearest neighbor
							String clusterId = (String) obj.get("clusterId");
							
							logger.info(id + " -> " + nearestId + " (" + nearestId + ")");
							
							BasicDBObject q = new BasicDBObject("id", id);
							BasicDBObject o = new BasicDBObject("$set", new BasicDBObject("clusterId", clusterId));
							_mediaItemsCollection.update(q, o, true, false);
							
							mediaClusterDAO.addMediaItemInCluster(clusterId, id);
						}
						else {
							if(obj == null) {
								logger.error("Error: " + nearestId + " not found!");
							}
							else {
								logger.error("Error: " + nearestId + " not clustered!");
							}
						}
					}
					else {
						// Create new Cluster
						UUID clusterId = UUID.randomUUID();
						
						BasicDBObject q = new BasicDBObject("id", id);
						BasicDBObject o = new BasicDBObject("$set", new BasicDBObject("clusterId", clusterId.toString()));
						_mediaItemsCollection.update(q, o, true, false);
						
						MediaCluster cluster = new MediaCluster(clusterId.toString());
						cluster.addMember(id);
			
						mediaClusterDAO.addMediaCluster(cluster);
					}
				}
			}
		}
		
	}
	
}