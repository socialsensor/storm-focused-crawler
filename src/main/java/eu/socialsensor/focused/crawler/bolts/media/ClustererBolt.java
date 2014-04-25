package eu.socialsensor.focused.crawler.bolts.media;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import eu.socialsensor.framework.client.dao.MediaClusterDAO;
import eu.socialsensor.framework.client.dao.MediaItemDAO;
import eu.socialsensor.framework.client.dao.impl.MediaClusterDAOImpl;
import eu.socialsensor.framework.client.dao.impl.MediaItemDAOImpl;
import eu.socialsensor.framework.client.search.visual.JsonResultSet;
import eu.socialsensor.framework.client.search.visual.JsonResultSet.JsonResult;
import eu.socialsensor.framework.client.search.visual.VisualIndexHandler;
import eu.socialsensor.framework.common.domain.MediaCluster;
import eu.socialsensor.framework.common.domain.MediaItem;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 *	@author Manos Schinas - manosetro@iti.gr
 *
 *
 *
 */
public class ClustererBolt extends BaseRichBolt {

	private static final long serialVersionUID = -2548434425109192911L;
	
	Logger logger;
	
	private String mongoHost;
	
	private String mediaItemsDbName;
	private String mediaItemsCollectionName;
	
	private String clustersDbName;
	private String clustersCollectionName;
	
	private MediaItemDAO _mediaItemDAO;
	private MediaClusterDAO _mediaClusterDAO;

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
	
	public ClustererBolt(String mongoHost, String mediaItemsDbName, String mediaItemsCollectionName, String clustersDbName, 
			String clustersCollectionName, String indexHostname, String indexCollection, double threshold ) {
		
		this(mongoHost, mediaItemsDbName, mediaItemsCollectionName, clustersDbName, clustersCollectionName, indexHostname, indexCollection);
		
		this.threshold = threshold;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		
		logger = Logger.getLogger(ClustererBolt.class);
		
		try {
			
			_mediaItemDAO = new MediaItemDAOImpl(mongoHost, mediaItemsDbName, mediaItemsCollectionName);
			_mediaClusterDAO = new MediaClusterDAOImpl(mongoHost, clustersDbName, clustersCollectionName);
			
			_visualIndex = new VisualIndexHandler(indexHostname, indexCollection);
			
			Thread thread = new Thread(new Clusterer(_mQ));
			thread.start();
			
		} catch (Exception e) {
			logger.error(e);
		}
		
	}

	public void execute(Tuple tuple) {
		try {
			MediaItem mediaItem = (MediaItem) tuple.getValueByField("MediaItem");
			String id = mediaItem.getId();
			
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
		catch(Exception e) {
			logger.error(e);
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
					
						MediaItem nearestMediaItem = _mediaItemDAO.getMediaItem(nearestId.toString());
						if(nearestMediaItem != null && nearestMediaItem.getClusterId() != null) {
							
							// Add media item to the same cluster as the nearest neighbor
							String clusterId = nearestMediaItem.getClusterId();
							
							logger.info(id + " -> " + nearestId + " (" + nearestId + ")");
							
							_mediaItemDAO.updateMediaItem(id, "clusterId", clusterId);
							_mediaClusterDAO.addMediaItemInCluster(clusterId, id);
						}
						else {
							if(nearestMediaItem == null) {
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
						
						_mediaItemDAO.updateMediaItem(id, "clusterId", clusterId);
						
						MediaCluster cluster = new MediaCluster(clusterId.toString());
						cluster.addMember(id);
						_mediaClusterDAO.addMediaCluster(cluster);
					}
				}
			}
		}
	}
}