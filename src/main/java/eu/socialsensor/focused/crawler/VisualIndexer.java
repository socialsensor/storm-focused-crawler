package eu.socialsensor.focused.crawler;

import java.net.UnknownHostException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;

import eu.socialsensor.focused.crawler.bolts.media.ConceptDetectionBolt;
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
import backtype.storm.topology.base.BaseRichSpout;

public class VisualIndexer {

	/**
	 * @param args
	 * @throws UnknownHostException 
	 */
	public static void main(String[] args) throws UnknownHostException {
		
		Logger logger = Logger.getLogger(VisualIndexer.class);
		
		XMLConfiguration config;
		try {
			if(args.length == 1)
				config = new XMLConfiguration(args[0]);
			else
				config = new XMLConfiguration("./conf/focused.crawler.xml");
		}
		catch(ConfigurationException ex) {
			logger.error(ex);
			return;
		}
		
		String redisHost = config.getString("redis.hostname", "xxx.xxx.xxx.xxx");
		String redisMediaChannel = config.getString("redis.mediaItemsChannel", "media");
		
		
		String mongodbHostname = config.getString("mongodb.hostname", "xxx.xxx.xxx.xxx");
		String mediaItemsDB = config.getString("mongodb.mediaItemsDB", "Prototype");
		String mediaItemsCollection = config.getString("mongodb.mediaItemsCollection", "MediaItems_WP");
		String streamUsersDB = config.getString("mongodb.streamUsersDB", "Prototype");
		String streamUsersCollection = config.getString("mongodb.streamUsersCollection", "StreamUsers");
		String clustersDB = config.getString("mongodb.clustersDB", "Prototype");
		String clustersCollection = config.getString("mongodb.clustersCollection", "MediaClusters");
		
		String conceptDetectorMatlabfile = config.getString("conceptdetector.matlabfile");
		
		String visualIndexHostname = config.getString("visualindex.hostname");
		String visualIndexCollection = config.getString("visualindex.collection");
		
		String learningFiles = config.getString("visualindex.learningfiles");
		if(!learningFiles.endsWith("/"))
			learningFiles = learningFiles + "/";
		
		String[] codebookFiles = { 
				learningFiles + "surf_l2_128c_0.csv",
				learningFiles + "surf_l2_128c_1.csv", 
				learningFiles + "surf_l2_128c_2.csv",
				learningFiles + "surf_l2_128c_3.csv" };
		
		String pcaFile = learningFiles + "pca_surf_4x128_32768to1024.txt";
		
		BaseRichSpout miSpout;
		IRichBolt miRanker, visualIndexer, clusterer, mediaUpdater, conceptDetector;
		
		try {
			miSpout = new RedisSpout(redisHost, redisMediaChannel, "id");	
			miRanker = new MediaRankerBolt(redisMediaChannel);
					
			visualIndexer = new VisualIndexerBolt(visualIndexHostname, visualIndexCollection, codebookFiles, pcaFile);
			//clusterer = new ClustererBolt(mongodbHostname, mediaItemsDB, mediaItemsCollection, clustersDB, clustersCollection, visualIndexHostname, visualIndexCollection);
			conceptDetector = new ConceptDetectionBolt(conceptDetectorMatlabfile);
			
			mediaUpdater = new MediaUpdaterBolt(mongodbHostname, mediaItemsDB, mediaItemsCollection, streamUsersDB, streamUsersCollection);
		} catch (Exception e) {
			logger.error(e);
			return;
		}
		
		// Create topology 
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("miInjector", miSpout, 1);
		
		builder.setBolt("miRanker", miRanker, 4).shuffleGrouping("miInjector");

        builder.setBolt("indexer", visualIndexer, 16).shuffleGrouping("miRanker");
        //builder.setBolt("clusterer", clusterer, 1).shuffleGrouping("indexer");   
        builder.setBolt("conceptDetector", conceptDetector, 1).shuffleGrouping("indexer");
        
		builder.setBolt("mediaupdater", mediaUpdater, 1)
			.shuffleGrouping("conceptDetector");
		
        // Run topology
        String name = config.getString("topology.focusedCrawlerName", "VisualIndexer");
        boolean local = config.getBoolean("topology.local", true);
        
        Config conf = new Config();
        conf.setDebug(false);
        
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