package eu.socialsensor.focused.crawler.bolts.media;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
 */
public class ClustererBolt extends BaseRichBolt {

	private static final long serialVersionUID = -2548434425109192911L;
	
	Logger logger;
	
	private String mongoHost;
	
	private String mediaItemsDbName;
	private String mediaItemsCollectionName;
	
	private String clustersDbName;
	private String clustersCollectionName;
	
	private MediaItemDAO _mediaItemDAO = null;
	private MediaClusterDAO _mediaClusterDAO = null;

	private Queue<Pair<?, ?>> _mQ = new LinkedList<Pair<?, ?>>();

	private Map<String, String> newClusters = new HashMap<String, String>();
	private Map<String, String> existingClusters = new HashMap<String, String>();
	
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
	
	public ClustererBolt(String mongoHost, String mediaItemsDbName, String mediaItemsCollectionName, String indexHostname, String indexCollection ) {
		
		this(mongoHost, mediaItemsDbName, mediaItemsCollectionName, null, null, indexHostname, indexCollection);
		
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		
		logger = Logger.getLogger(ClustererBolt.class);
		
		try {
			
			_mediaItemDAO = new MediaItemDAOImpl(mongoHost, mediaItemsDbName, mediaItemsCollectionName);
			
			if(clustersDbName != null && clustersCollectionName != null) {
				_mediaClusterDAO = new MediaClusterDAOImpl(mongoHost, clustersDbName, clustersCollectionName);
			}
			
			_visualIndex = new VisualIndexHandler(indexHostname, indexCollection);
			
			Thread clustererThread = new Thread(new Clusterer(_mQ));
			clustererThread.start();
			
			Thread updaterThread = new Thread(new Updater());
			updaterThread.start();
			
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
						synchronized(existingClusters) {
							existingClusters.put(id, (String) nearestId);
						}
					}
					else {
						// Create new Cluster
						UUID clusterId = UUID.randomUUID();
						synchronized(newClusters) {
							newClusters.put(id, clusterId.toString());
						}
					}
				}
			}
		}
	}
	
	public class Updater implements Runnable {

		@Override
		public void run() {
			while(true) {
				try {
					Thread.sleep(5 * 60 * 1000);
				} catch (InterruptedException e) {
					logger.error(e);
				}
				
				Map<String, String> existingC = new HashMap<String, String>();
				synchronized(existingClusters) {
					existingC.putAll(existingClusters);
					existingClusters.clear();
				}
				
				Map<String, String> newC = new HashMap<String, String>();
				synchronized(newClusters) {
					newC.putAll(newClusters);
					newClusters.clear();
				}
				
				// Store new clusters
				for(String mId : newC.keySet()) {
					
					String clusterId = newC.get(mId);
					
					_mediaItemDAO.updateMediaItem(mId, "clusterId", clusterId);
					
					MediaCluster cluster = new MediaCluster(clusterId.toString());
					cluster.addMember(mId);
					
					if(_mediaClusterDAO != null)
						_mediaClusterDAO.addMediaCluster(cluster);
				}
				
				// Update media items with cluster id and clusters with new members
				for(Entry<String, String> e : existingC.entrySet()) {
					
					String id = e.getKey();
					String nearestMediaId = e.getValue();
					
					String clusterId = null;
					if(newC.containsKey(id)) {
						clusterId = newC.get(id);
					}
					else {
						MediaItem nearestMediaItem = _mediaItemDAO.getMediaItem(nearestMediaId);
						if(nearestMediaItem != null) {
							clusterId = nearestMediaItem.getClusterId();
						}
						else {
							logger.error("Error: " + nearestMediaId + " not found!");
							continue;
						}
					}
					
					if(clusterId != null) {
						
						logger.info(id + " -> Cluster: " + clusterId + " ( nearest: " + nearestMediaId + " )");
						
						_mediaItemDAO.updateMediaItem(id, "clusterId", clusterId);
						
						if(_mediaClusterDAO != null)
							_mediaClusterDAO.addMediaItemInCluster(clusterId, id);
					}
					else {
						logger.error("Error: " + nearestMediaId + " not clustered!");
						
					}
				}
			}
			
		}
	}
	
	public class ClustererOld implements Runnable {

		private Queue<Pair<?, ?>> queue;
		
		public ClustererOld(Queue<Pair<?, ?>> queue) {
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
							
							if(_mediaClusterDAO != null)
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
						
						if(_mediaClusterDAO != null)
							_mediaClusterDAO.addMediaCluster(cluster);
					}
				}
			}
		}
	}
}