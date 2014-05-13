package eu.socialsensor.focused.crawler;

import java.net.UnknownHostException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;

import eu.socialsensor.focused.crawler.bolts.media.ConceptDetectionBolt;
import eu.socialsensor.focused.crawler.bolts.media.DeserializerMediaItemBolt;
import eu.socialsensor.focused.crawler.bolts.media.MediaTextIndexerBolt;
import eu.socialsensor.focused.crawler.bolts.media.MediaUpdaterBolt;
import eu.socialsensor.focused.crawler.bolts.media.VisualIndexerBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.ArticleExtractionBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.DeserializerWebPageBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.MediaExtractionBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.TextIndexerBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.URLExpansionBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.WebPagesUpdaterBolt;
import eu.socialsensor.focused.crawler.spouts.RedisSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;

/**
 *	@author Manos Schinas - manosetro@iti.gr
 *
 *	Entry class for SocialSensor focused crawling.  
 *  This class defines a storm-based pipeline (topology) for the processing of MediaItems and WebPages
 *  collected by StreamManager.
 *  
 *  For more information on Storm distributed processing check this tutorial:
 *  https://github.com/nathanmarz/storm/wiki/Tutorial
 *  
 */
public class SocialsensorCrawler {

	private static Logger logger = Logger.getLogger(SocialsensorCrawler.class);
	
	public static void main(String[] args) throws UnknownHostException {
		
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
		
		StormTopology topology;
		try {
			topology = createTopology(config);
		} catch (Exception e) {
			logger.error("Topology Creation failed: ", e);
			return;
		}
		
        // Run topology
        String name = "SocialsensorCrawler";
        boolean local = config.getBoolean("topology.local", true);
        
        Config conf = new Config();
        conf.setDebug(false);
        
        if(!local) {
        	logger.info("Submit topology to Storm cluster");
			try {
				int workers = config.getInt("topology.workers", 2);
				conf.setNumWorkers(workers);
				
				StormSubmitter.submitTopology(name, conf, topology);
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
			try {
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology(name, conf, topology);
			}
			catch(Exception e) {
				logger.error(e);
			}
		}
	}
	
	public static StormTopology createTopology(XMLConfiguration config) throws Exception {
		
		// Get Params from config file
		String redisHost = config.getString("redis.hostname", "xxx.xxx.xxx.xxx");
		String webPagesChannel = config.getString("redis.webPagesChannel", "webpages");
		String mediaItemsChannel = config.getString("redis.mediaItemsChannel", "media");
		
		String mongodbHostname = config.getString("mongodb.hostname", "xxx.xxx.xxx.xxx");
		String mediaItemsDB = config.getString("mongodb.mediaItemsDB", "Prototype");
		String mediaItemsCollection = config.getString("mongodb.mediaItemsCollection", "MediaItems_WP");
		String streamUsersDB = config.getString("mongodb.streamUsersDB", "Prototype");
		String streamUsersCollection = config.getString("mongodb.streamUsersCollection", "StreamUsers");
		String webPagesDB = config.getString("mongodb.webPagesDB", "Prototype");
		String webPagesCollection = config.getString("mongodb.webPagesCollection", "WebPages");
		//String clustersDB = config.getString("mongodb.clustersDB", "Prototype");
		//String clustersCollection = config.getString("mongodb.clustersCollection", "MediaClusters");
		
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
		
		String textIndexHostname = config.getString("textindex.hostname", "http://xxx.xxx.xxx.xxx:8080/solr");
		String textIndexCollection = config.getString("textindex.collections.webpages", "WebPages");
		String textIndexService = textIndexHostname + "/" + textIndexCollection;
		
		String mediaTextIndexCollection = config.getString("textindex.collections.media", "MediaItems");
		String mediaTextIndexService = textIndexHostname + "/" + mediaTextIndexCollection;
		
		String conceptDetectorMatlabfile = config.getString("conceptdetector.matlabfile");
		
		// Initialize spouts and bolts
		BaseRichSpout wpSpout, miSpout;
		IRichBolt wpDeserializer, miDeserializer;
		IRichBolt urlExpander, articleExtraction, mediaExtraction;
		IRichBolt mediaUpdater, webPageUpdater, textIndexer;
		IRichBolt visualIndexer, mediaTextIndexer, conceptDetector;
		
		wpSpout = new RedisSpout(redisHost, webPagesChannel, "url");
		miSpout = new RedisSpout(redisHost, mediaItemsChannel, "id");
			
		//wpRanker = new RankerBolt(webPagesChannel);
		//miRanker = new MediaRankerBolt(mediaItemsChannel);
			
		wpDeserializer = new DeserializerWebPageBolt(webPagesChannel);
		miDeserializer = new DeserializerMediaItemBolt(mediaItemsChannel);
		
		// Web Pages Bolts
		urlExpander = new URLExpansionBolt("webpages");
		articleExtraction = new ArticleExtractionBolt(24);
		mediaExtraction = new MediaExtractionBolt();
		webPageUpdater = new WebPagesUpdaterBolt(mongodbHostname, webPagesDB, webPagesCollection);
		textIndexer = new TextIndexerBolt(textIndexService);
			
		// Media Items Bolts
		visualIndexer = new VisualIndexerBolt(visualIndexHostname, visualIndexCollection, codebookFiles, pcaFile);
		//clusterer = new ClustererBolt(mongodbHostname, mediaItemsDB, mediaItemsCollection, clustersDB, clustersCollection, visualIndexHostname, visualIndexCollection);
		conceptDetector = new ConceptDetectionBolt(conceptDetectorMatlabfile);
		mediaUpdater = new MediaUpdaterBolt(mongodbHostname, mediaItemsDB, mediaItemsCollection, streamUsersDB, streamUsersCollection);
		mediaTextIndexer = new MediaTextIndexerBolt(mediaTextIndexService);
		
		
		// Create topology 
		TopologyBuilder builder = new TopologyBuilder();
		
		// Input in topology
		builder.setSpout("wpSpout", wpSpout, 1);
		builder.setSpout("miSpout", miSpout, 1);
		
		// Web Pages Bolts
		//builder.setBolt("wpRanker", wpRanker, 2).shuffleGrouping("wpSpout");
		builder.setBolt("wpDeserializer", wpDeserializer, 2).shuffleGrouping("wpSpout");
		builder.setBolt("expander", urlExpander, 8).shuffleGrouping("wpDeserializer");
		builder.setBolt("articleExtraction", articleExtraction, 1).shuffleGrouping("expander", "webpage");
		builder.setBolt("mediaExtraction", mediaExtraction, 1).shuffleGrouping("expander", "media");
		builder.setBolt("webPageUpdater", webPageUpdater, 1)
			.shuffleGrouping("articleExtraction", "webpage")
			.shuffleGrouping("mediaExtraction", "webpage");
		builder.setBolt("textIndexer", textIndexer, 1)
			.shuffleGrouping("articleExtraction", "webpage");

		// Media Items Bolts
		//builder.setBolt("miRanker", miRanker, 2).shuffleGrouping("miSpout");
		builder.setBolt("miDeserializer", miDeserializer, 2).shuffleGrouping("miSpout");
        builder.setBolt("indexer", visualIndexer, 16)
        	.shuffleGrouping("miDeserializer")
			.shuffleGrouping("articleExtraction", "media")
			.shuffleGrouping("mediaExtraction", "media");
        
        builder.setBolt("conceptDetector", conceptDetector, 1).shuffleGrouping("indexer");
        //builder.setBolt("clusterer", clusterer, 1).shuffleGrouping("indexer");   
        builder.setBolt("mediaupdater", mediaUpdater, 1).shuffleGrouping("conceptDetector");
		builder.setBolt("mediaTextIndexer", mediaTextIndexer, 1).shuffleGrouping("mediaupdater");
		
		StormTopology topology = builder.createTopology();
		return topology;
		
	}
	
}