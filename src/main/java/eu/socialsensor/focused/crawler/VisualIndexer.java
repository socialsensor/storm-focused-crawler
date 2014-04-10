package eu.socialsensor.focused.crawler;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;

import eu.socialsensor.focused.crawler.bolts.media.MediaRankerBolt;
import eu.socialsensor.focused.crawler.bolts.media.MediaUpdaterBolt;
import eu.socialsensor.focused.crawler.bolts.media.VisualIndexerBolt;
import eu.socialsensor.focused.crawler.spouts.RedisSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.TopologyBuilder;

public class VisualIndexer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Logger logger = Logger.getLogger(VisualIndexer.class);
		
		XMLConfiguration config;
		try {
			if(args.length == 1)
				config = new XMLConfiguration(args[0]);
			else
				config = new XMLConfiguration("./conf/focused.crawler.xml");
		}
		catch(ConfigurationException ex) {
			return;
		}
		
		String redisHost = config.getString("redis.host", "xxx.xxx.xxx.xxx");
		
		String mongoHost = config.getString("mongodb.host", "xxx.xxx.xxx.xxx");
		String mongoDbName = config.getString("mongodb.db");
		String mediaItemsCollection = config.getString("mongodb.media", "MediaItems");
		String clustersCollection = config.getString("mongodb.clusters", "MediaClusters");
		
		String indexHostname = config.getString("visualindex.host");
		String indexCollection = config.getString("visualindex.collection");
		
		String learningFiles = config.getString("visualindex.learningfiles");
		if(!learningFiles.endsWith("/"))
			learningFiles = learningFiles + "/";
		
		String[] codebookFiles = { 
				learningFiles + "surf_l2_128c_0.csv",
				learningFiles + "surf_l2_128c_1.csv", 
				learningFiles + "surf_l2_128c_2.csv",
				learningFiles + "surf_l2_128c_3.csv" };
		
		String pcaFile = learningFiles + "pca_surf_4x128_32768to1024.txt";
	
		
		RedisSpout rediSpout = new RedisSpout(redisHost, "media", "id");
		
		IRichBolt visualIndexer, ranker, updater;
		try {
			visualIndexer = new VisualIndexerBolt(indexHostname, indexCollection, codebookFiles, pcaFile);
			ranker = new MediaRankerBolt("media");
			updater = new MediaUpdaterBolt(mongoHost, mongoDbName, mediaItemsCollection, clustersCollection, indexHostname, indexCollection);
		} catch (Exception e) {
			return;
		}
		
		TopologyBuilder builder = new TopologyBuilder();
        
		builder.setSpout("injector", rediSpout, 1);
        builder.setBolt("ranker", ranker, 4).shuffleGrouping("injector");
        builder.setBolt("indexer", visualIndexer, 16).shuffleGrouping("ranker");
		builder.setBolt("updater", updater, 1).shuffleGrouping("indexer");
        
        Config conf = new Config();
        conf.setDebug(false);
        
        // Run topology
        String name = config.getString("topology.name", "visual-indexer");
        boolean local = config.getBoolean("topology.local", true);
        
        if(!local) {
        	System.out.println("Submit topology to Storm cluster");
			try {
				int workers = config.getInt("topology.workers", 2);
				conf.setNumWorkers(workers);
				
				StormSubmitter.submitTopology(name, conf, builder.createTopology());
			}
			catch(NumberFormatException e) {
				logger.error(e);
			} catch (AlreadyAliveException e) {
				logger.error(e);
			} catch (InvalidTopologyException e) {
				logger.error(e);
			}
			
		} else {
			logger.info("Run topology in local mode");
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(name, conf, builder.createTopology());
		}
        
	}

}
