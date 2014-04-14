package eu.socialsensor.focused.crawler;

import java.net.UnknownHostException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;

import eu.socialsensor.focused.crawler.bolts.media.ClustererBolt;
import eu.socialsensor.focused.crawler.bolts.media.MediaRankerBolt;
import eu.socialsensor.focused.crawler.bolts.media.VisualIndexerBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.ArticleExtractionBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.MediaExtractionBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.RankerBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.RedisBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.URLExpanderBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.UpdaterBolt;
import eu.socialsensor.focused.crawler.spouts.RedisSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;

public class SocialsensorCrawler {

	/**
	 * @param args
	 * @throws UnknownHostException 
	 */
	public static void main(String[] args) throws UnknownHostException {
		
		Logger logger = Logger.getLogger(FocusedCrawler.class);
		
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
		
		String mongodbHostname = config.getString("mongodb.hostname", "xxx.xxx.xxx.xxx");
		String mediaItemsDB = config.getString("mongodb.mediaItemsDB", "Prototype");
		String mediaItemsCollection = config.getString("mongodb.mediaItemsCollection", "MediaItems_WP");
		String webPagesDB = config.getString("mongodb.webPagesDB", "Prototype");
		String webPagesCollection = config.getString("mongodb.webPagesCollection", "WebPages");
		String clustersDB = config.getString("mongodb.clustersDB", "Prototype");
		String clustersCollection = config.getString("mongodb.clustersCollection", "MediaClusters");
		
		
		String indexHostname = config.getString("visualindex.hostname");
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
		
		//String textIndexHostname = config.getString("textindex.host", "xxx.xxx.xxx.xxx:8080/solr");
		//String textIndexCollection = config.getString("textindex.collection", "WebPagesP");
		
		BaseRichSpout wpSpout, miSpout;
		IRichBolt wpRanker, miRanker;
		IRichBolt urlExpander, articleExtraction, mediaExtraction, updater;
		IRichBolt visualIndexer, clusterer;
		IRichBolt miEmitter;
		
		try {
			//BaseRichSpout spout = new MongoDbSpout(mongodbHostname, mongoDBName, webPagesCollection, query);
			wpSpout = new RedisSpout(redisHost, "webpages", "url");
			miSpout = new RedisSpout(redisHost, "media", "id");
			
			wpRanker = new RankerBolt("webpages");
			miRanker = new MediaRankerBolt("media");
			
			urlExpander = new URLExpanderBolt("webpages");
			
			articleExtraction = new ArticleExtractionBolt(48);
			mediaExtraction = new MediaExtractionBolt();
			updater = new UpdaterBolt(mongodbHostname, mediaItemsDB, mediaItemsCollection, webPagesDB, webPagesCollection);
			
			miEmitter = new RedisBolt(redisHost, "media");
					
			visualIndexer = new VisualIndexerBolt(indexHostname, indexCollection, codebookFiles, pcaFile);
			clusterer = new ClustererBolt(mongodbHostname, mediaItemsDB, mediaItemsCollection, clustersDB, clustersCollection, indexHostname, indexCollection);
		} catch (Exception e) {
			logger.error(e);
			return;
		}
		
		
		
		// Create topology 
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("wpInjector", wpSpout, 1);
		builder.setSpout("miInjector", miSpout, 1);
		
		builder.setBolt("wpRanker", wpRanker, 4).shuffleGrouping("wpInjector");
		builder.setBolt("miRanker", miRanker, 4).shuffleGrouping("miInjector");
		
		builder.setBolt("expander", urlExpander, 8).shuffleGrouping("wpRanker");
		builder.setBolt("articleExtraction", articleExtraction, 1).shuffleGrouping("expander", "article");
		builder.setBolt("mediaExtraction", mediaExtraction, 4).shuffleGrouping("expander", "media");
		builder.setBolt("updater", updater, 4).shuffleGrouping("articleExtraction").shuffleGrouping("mediaExtraction");
		builder.setBolt("miEmitter", miEmitter, 1).shuffleGrouping("updater");
		
		
        builder.setBolt("indexer", visualIndexer, 16).shuffleGrouping("miRanker");
        builder.setBolt("clusterer", clusterer, 1).shuffleGrouping("indexer");
        
		//String textIndexService = textIndexHostname + "/" + textIndexCollection;
		//WebPagesIndexerBolt indexer = new WebPagesIndexerBolt(textIndexService, mongodbHostname, mongoDBName, webPagesCollection);
		//builder.setBolt("text-indexer", indexer, 1).shuffleGrouping("updater");
		       
        // Run topology
        String name = config.getString("topology.focusedCrawlerName", "FocusedCrawler");
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