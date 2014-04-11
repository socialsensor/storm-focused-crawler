package eu.socialsensor.focused.crawler;

import java.net.UnknownHostException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;

import eu.socialsensor.focused.crawler.bolts.webpages.ArticleExtractionBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.MediaExtractionBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.RankerBolt;
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

public class FocusedCrawler {

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
		
		
		//String textIndexHostname = config.getString("textindex.host", "xxx.xxx.xxx.xxx:8080/solr");
		//String textIndexCollection = config.getString("textindex.collection", "WebPagesP");
		
		BaseRichSpout spout;
		IRichBolt urlExpander, ranker, articleExtraction, mediaExtraction, updater;
		try {
			//BaseRichSpout spout = new MongoDbSpout(mongodbHostname, mongoDBName, webPagesCollection, query);
			spout = new RedisSpout(redisHost, "webpages", "url");
			
			urlExpander = new URLExpanderBolt(mongodbHostname, webPagesDB, webPagesCollection, "webpages");
			ranker = new RankerBolt("webpages");
			articleExtraction = new ArticleExtractionBolt(48);
			mediaExtraction = new MediaExtractionBolt();
			updater = new UpdaterBolt(mongodbHostname, mediaItemsDB, mediaItemsCollection, webPagesDB, webPagesCollection);
		} catch (Exception e) {
			logger.error(e);
			return;
		}
		
		
		
		// Create topology 
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("injector", spout, 1);
        
		builder.setBolt("ranker", ranker, 4).shuffleGrouping("injector");
		builder.setBolt("expander", urlExpander, 8).shuffleGrouping("ranker");
		builder.setBolt("articleExtraction", articleExtraction, 1).shuffleGrouping("expander", "article");
		builder.setBolt("mediaExtraction", mediaExtraction, 4).shuffleGrouping("expander", "media");
		
		builder.setBolt("updater", updater, 4).shuffleGrouping("articleExtraction").shuffleGrouping("mediaExtraction");
		
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