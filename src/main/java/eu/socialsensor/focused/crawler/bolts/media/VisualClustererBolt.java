package eu.socialsensor.focused.crawler.bolts.media;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
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
public class VisualClustererBolt extends BaseRichBolt {

	private static final long serialVersionUID = -2548434425109192911L;
	
	private Logger logger;
	
	private String redisHost;
	private String solrHost;
	
	private Queue<Pair<?, ?>> _mQ = new LinkedBlockingQueue<Pair<?, ?>>();
	private Map<String, String> clusters = new ConcurrentHashMap<String, String>();
	
	private Jedis jedis;
	private HttpSolrServer solrServer;
	
	public VisualClustererBolt(String redisHost, String solrHost) {
		this.redisHost = redisHost;
		this.solrHost = solrHost;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		
		logger = Logger.getLogger(VisualClustererBolt.class);
		
		try {
			
			JedisPool pool = new JedisPool(new JedisPoolConfig(), redisHost);
			jedis = pool.getResource();
			
			solrServer = new HttpSolrServer(solrHost);
			
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

			String nearestId =  tuple.getStringByField("nearestMediaItem");
			
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
					
					String clusterId = null;
					if(nearestId != null) {
						clusterId = jedis.hget((String) nearestId, "CLUSTER");
						if(clusterId == null) {
							clusterId = UUID.randomUUID().toString();
						}
					}
					else {
						// Create new Cluster
						clusterId = UUID.randomUUID().toString();
					}
					
					
					if(clusterId != null) {
						jedis.hset(id, "CLUSTER", clusterId);
						clusters.put(id, clusterId);
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
				
				try {
					
					Map<String, String> clustersToUpdate = new ConcurrentHashMap<String, String>();
					synchronized(clusters) {
						clustersToUpdate.putAll(clusters);
						clusters.clear();
					}
				
					List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
					// Store new clusters
					for(String mId : clustersToUpdate.keySet()) {
						String clusterId = clustersToUpdate.get(mId);

						SolrInputDocument doc = new SolrInputDocument();
						doc.addField("id", mId);
						
						Map<String, String> partialUpdate = new HashMap<String, String>();
						partialUpdate.put("set", clusterId);
						
						doc.addField("clusterId", partialUpdate);
						
						docs.add(doc);
					}
					
					solrServer.add(docs);
					solrServer.commit();
				
				} catch (Exception e) {
					logger.error(e);
				}
				

			}
		}
	}
	
}