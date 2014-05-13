package eu.socialsensor.focused.crawler;

import java.net.UnknownHostException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;

import eu.socialsensor.focused.crawler.bolts.media.MediaTextIndexerBolt;
import eu.socialsensor.focused.crawler.bolts.media.MediaUpdaterBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.ArticleExtractionBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.MediaExtractionBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.RankerBolt;
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

public class FocusedCrawler {

	private static Logger logger = Logger.getLogger(FocusedCrawler.class);
	
	
	/**
	 *	@author Manos Schinas - manosetro@iti.gr
	 *
	 *	Entry class for distributed web page processing. 
	 *  This class defines a storm-based pipeline (topology) for the processing of WebPages 
	 *  received from a Redis pub/sub channel. 
	 *  
	 * 	The main steps in the topology are: URLExpansion, ArticleExctraction, MediaExtraction,
	 *  WebPage Text Indexing and Media Text Indexing. 
	 *  
	 *  For more information on Storm distributed processing check this tutorial:
	 *  https://github.com/nathanmarz/storm/wiki/Tutorial
	 *  
	 */
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
	
		StormTopology topology = null;
		try {
			topology = createTopology(config);
		}
		catch(Exception e) {
			logger.error(e);
		}
		
        // Run topology
        String name = config.getString("topology.focusedCrawlerName", "FocusedCrawler");
        boolean local = config.getBoolean("topology.local", true);
        
        Config conf = new Config();
        conf.setDebug(false);
        
        if(!local) {
        	System.out.println("Submit topology to Storm cluster");
			try {
				int workers = config.getInt("topology.workers", 4);
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
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(name, conf, topology);
		}
	}
	
	public static StormTopology createTopology(XMLConfiguration config) {
	
		String redisHost = config.getString("redis.hostname", "xxx.xxx.xxx.xxx");
		String webPagesChannel = config.getString("redis.webPagesChannel", "webpages");
		
		String mongodbHostname = config.getString("mongodb.hostname", "xxx.xxx.xxx.xxx");
		String mediaItemsDB = config.getString("mongodb.mediaItemsDB", "Prototype");
		String mediaItemsCollection = config.getString("mongodb.mediaItemsCollection", "MediaItems");
		String streamUsersDB = config.getString("mongodb.streamUsersDB", "Prototype");
		String streamUsersCollection = config.getString("mongodb.streamUsersCollection", "StreamUsers");
		String webPagesDB = config.getString("mongodb.webPagesDB", "Prototype");
		String webPagesCollection = config.getString("mongodb.webPagesCollection", "WebPages");
		
		String textIndexHostname = config.getString("textindex.hostname", "xxx.xxx.xxx.xxx:8080/solr");
		String textIndexCollection = config.getString("textindex.collections/webpages", "WebPages");
		String mediaIndexCollection = config.getString("textindex.collections/webpages", "MediaItems");
		String textIndexService = textIndexHostname + "/" + textIndexCollection;
		String mediaTextIndexService = textIndexHostname + "/" + mediaIndexCollection;
		
		BaseRichSpout wpSpout;
		IRichBolt wpRanker, mediaUpdater, urlExpander, mediaTextIndexer;
		IRichBolt articleExtraction, mediaExtraction, webPageUpdater, textIndexer;
		
		try {
			wpSpout = new RedisSpout(redisHost, webPagesChannel, "url");
			wpRanker = new RankerBolt(webPagesChannel);
			urlExpander = new URLExpansionBolt(webPagesChannel);
			
			articleExtraction = new ArticleExtractionBolt(24);
			mediaExtraction = new MediaExtractionBolt();
			webPageUpdater = new WebPagesUpdaterBolt(mongodbHostname, webPagesDB, webPagesCollection);
			textIndexer = new TextIndexerBolt(textIndexService);

			mediaUpdater = new MediaUpdaterBolt(mongodbHostname, mediaItemsDB, mediaItemsCollection, streamUsersDB, streamUsersCollection);
			mediaTextIndexer = new MediaTextIndexerBolt(mediaTextIndexService);
		} catch (Exception e) {
			logger.error(e);
			return null;
		}
		
		// Create topology 
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("wpSpout", wpSpout, 1);
				
		builder.setBolt("wpRanker", wpRanker, 4).shuffleGrouping("wpSpout");
		builder.setBolt("expander", urlExpander, 8).shuffleGrouping("wpRanker");
				
				
		builder.setBolt("articleExtraction", articleExtraction, 1)
			.shuffleGrouping("expander", "webpage");
		builder.setBolt("mediaExtraction", mediaExtraction, 1)
			.shuffleGrouping("expander", "media");
				
		builder.setBolt("webPageUpdater", webPageUpdater, 4)
			.shuffleGrouping("articleExtraction", "webpage")
			.shuffleGrouping("mediaExtraction", "webpage");
				
		builder.setBolt("textIndexer", textIndexer, 1)
			.shuffleGrouping("articleExtraction", "webpage");
				
		builder.setBolt("mediaupdater", mediaUpdater, 1)
			.shuffleGrouping("articleExtraction", "media")
			.shuffleGrouping("mediaExtraction", "media");
		
		builder.setBolt("mediatextindexer", mediaTextIndexer, 1)
			.shuffleGrouping("articleExtraction", "media")
			.shuffleGrouping("mediaExtraction", "media");
		
		StormTopology topology = builder.createTopology();
		return topology;
	}
}