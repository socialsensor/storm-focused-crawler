package eu.socialsensor.focused.crawler.bolts.media;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import eu.socialsensor.focused.crawler.models.ImageVector;
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

	private Queue<Pair<?, ?>> _mQ = new LinkedBlockingQueue<Pair<?, ?>>();

	private Map<String, String> newClusters = new HashMap<String, String>();
	private Map<String, String> existingClusters = new HashMap<String, String>();
	
	private VisualIndexHandler _visualIndex;

	private String vIndexHostname;
	private String vIndexCollection;

	private String textIndexService;
	
	private double threshold = 0.75;

	private HttpSolrServer textIndexServiceHandler;
	
	public ClustererBolt(String mongoHost, String mediaItemsDbName, String mediaItemsCollectionName, String clustersDbName, 
			String clustersCollectionName, String vIndexHostname, String vIndexCollection, String textIndexService) {
		
		this.mongoHost = mongoHost;
		this.mediaItemsDbName = mediaItemsDbName;
		this.mediaItemsCollectionName = mediaItemsCollectionName;
		this.clustersDbName = clustersDbName;
		this.clustersCollectionName = clustersCollectionName;
		
		this.vIndexHostname = vIndexHostname; 
		this.vIndexCollection = vIndexCollection;
		
		this.textIndexService = textIndexService;
	}
	
	public ClustererBolt(String mongoHost, String mediaItemsDbName, String mediaItemsCollectionName, String clustersDbName, 
			String clustersCollectionName, String indexHostname, String indexCollection, String textIndexService, double threshold ) {
		
		this(mongoHost, mediaItemsDbName, mediaItemsCollectionName, clustersDbName, clustersCollectionName, indexHostname, indexCollection, textIndexService);
		this.threshold = threshold;
	}
	
	public ClustererBolt(String mongoHost, String mediaItemsDbName, String mediaItemsCollectionName, String indexHostname, String indexCollection, String textIndexService) {
		this(mongoHost, mediaItemsDbName, mediaItemsCollectionName, null, null, indexHostname, indexCollection, textIndexService);
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		
		logger = Logger.getLogger(ClustererBolt.class);
		
		try {
			_mediaItemDAO = new MediaItemDAOImpl(mongoHost, mediaItemsDbName, mediaItemsCollectionName);
			
			if(clustersDbName != null && clustersCollectionName != null) {
				_mediaClusterDAO = new MediaClusterDAOImpl(mongoHost, clustersDbName, clustersCollectionName);
			}
			
			_visualIndex = new VisualIndexHandler(vIndexHostname, vIndexCollection);
			
			textIndexServiceHandler = new HttpSolrServer(textIndexService);
			
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
			
			if(mediaItem == null)
				return;
			
			String id = mediaItem.getId();
			
			JsonResultSet response = _visualIndex.getSimilarImages(id, threshold);
			
			List<JsonResult> results = response.getResults();
			String nearestId = null;
			for(JsonResult result : results) {
				nearestId = result.getId();
				if(id.equals(nearestId)) {
					continue;
				}
			}
			_mQ.offer(Pair.of(id, nearestId));
			
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
					// Sleep one second
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
					// Wait 5 minutes & update
					Thread.sleep(5 * 60 * 1000);
				} catch (InterruptedException e) {
					logger.error(e);
				}
				
				Map<String, String> clustersToUpdate = new HashMap<String, String>();
				synchronized(existingClusters) {
					clustersToUpdate.putAll(existingClusters);
					existingClusters.clear();
				}
				
				Map<String, String> clustersToAdd = new HashMap<String, String>();
				synchronized(newClusters) {
					clustersToAdd.putAll(newClusters);
					newClusters.clear();
				}
				
				List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
				// Store new clusters
				for(String mId : clustersToAdd.keySet()) {
					String clusterId = clustersToAdd.get(mId);
					_mediaItemDAO.updateMediaItem(mId, "clusterId", clusterId);
					
					SolrInputDocument doc = new SolrInputDocument();
					doc.addField("id", mId);
					doc.addField("clusterId", clusterId);
			
					docs.add(doc);
					
					if(_mediaClusterDAO != null) {
						MediaCluster cluster = new MediaCluster(clusterId.toString());
						cluster.addMember(mId);
						_mediaClusterDAO.addMediaCluster(cluster);
					}
				}
				
				// Update media items with cluster id and clusters with new members
				for(Entry<String, String> e : clustersToUpdate.entrySet()) {
					
					String mId = e.getKey();
					String nearestMediaId = e.getValue();
					
					String clusterId = null;
					if(clustersToAdd.containsKey(mId)) {
						clusterId = clustersToAdd.get(mId);
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
						
						logger.info(mId + " -> Cluster: " + clusterId + " ( nearest: " + nearestMediaId + " )");
						
						_mediaItemDAO.updateMediaItem(mId, "clusterId", clusterId);
						
						SolrInputDocument doc = new SolrInputDocument();
						doc.addField("id", mId);
						doc.addField("clusterId", clusterId);
				
						docs.add(doc);
						
						if(_mediaClusterDAO != null) {
							_mediaClusterDAO.addMediaItemInCluster(clusterId, mId);
						}
						
					}
					else {
						logger.error("Error: " + nearestMediaId + " not clustered!");
						
					}
				}
				
				try {
					textIndexServiceHandler.add(docs);
					textIndexServiceHandler.commit();
				} catch (Exception e) {
					logger.error(e);
				}
			}
		}
	}
	
}