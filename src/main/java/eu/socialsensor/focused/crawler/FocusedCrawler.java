package eu.socialsensor.focused.crawler;

import java.net.UnknownHostException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import eu.socialsensor.focused.crawler.bolts.webpages.ArticleExtractionBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.MediaExtractionBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.RankerBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.URLExpanderBolt;
import eu.socialsensor.focused.crawler.bolts.webpages.UpdaterBolt;
import eu.socialsensor.focused.crawler.spouts.MongoDbSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class FocusedCrawler {

	/**
	 * @param args
	 * @throws UnknownHostException 
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
			return;
		}
		
		//String redisHost = config.getString("redis.host");
		
		String mongodbHostname = config.getString("mongodb.host", "xxx.xxx.xxx.xxx");
		String mongoDBName = config.getString("mongodb.db", "Prototype");
		String webPagesCollection = config.getString("mongodb.webpages", "WebPages");
		String mediaCollection = config.getString("mongodb.media", "MediaItems_WP");
		
		DBObject query = new BasicDBObject("status", "new");
		
		//String textIndexHostname = config.getString("textindex.host", "xxx.xxx.xxx.xxx:8080/solr");
		//String textIndexCollection = config.getString("textindex.collection", "WebPagesP");
		
		URLExpanderBolt urlExpander;
		try {
			urlExpander = new URLExpanderBolt(mongodbHostname, mongoDBName, webPagesCollection);
		} catch (Exception e) {
			return;
		}
		
		// Create topology 
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("injector", new MongoDbSpout(mongodbHostname, mongoDBName, webPagesCollection, query), 1);
        
		builder.setBolt("ranker", new RankerBolt("WebPages"), 1).shuffleGrouping("injector");
		builder.setBolt("expander", urlExpander, 8).shuffleGrouping("ranker");
		builder.setBolt("articleExtraction",  new ArticleExtractionBolt(60), 1).shuffleGrouping("expander", "article");
		builder.setBolt("mediaExtraction",  new MediaExtractionBolt(), 4).shuffleGrouping("expander", "media");
		
		builder.setBolt("updater",  new UpdaterBolt(mongodbHostname, mongoDBName, webPagesCollection, mediaCollection), 4)
			.shuffleGrouping("articleExtraction").shuffleGrouping("mediaExtraction");
		
		//String textIndexService = textIndexHostname + "/" + textIndexCollection;
		//WebPagesIndexerBolt indexer = new WebPagesIndexerBolt(textIndexService, mongodbHostname, mongoDBName, webPagesCollection);
		//builder.setBolt("text-indexer", indexer, 1).shuffleGrouping("updater");
		       
        // Run topology
        String name = config.getString("topology.name", "FocusedCrawler");
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
				System.out.println(e.getMessage());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
			
		} else {
			System.out.println("Run topology in local mode");
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(name, conf, builder.createTopology());
		}
	}
}