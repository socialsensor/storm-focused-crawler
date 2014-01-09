package eu.socialsensor.focused.crawler;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import eu.socialsensor.focused.crawler.bolts.ArticleExtractionBolt;
import eu.socialsensor.focused.crawler.bolts.MediaExtractionBolt;
import eu.socialsensor.focused.crawler.bolts.RankerBolt;
import eu.socialsensor.focused.crawler.bolts.URLExpanderBolt;
import eu.socialsensor.focused.crawler.bolts.UpdaterBolt;
import eu.socialsensor.focused.crawler.spouts.MongoDbInjector;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;


public class WebLinksHandler {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		XMLConfiguration config;
		try {
			if(args.length == 1)
				config = new XMLConfiguration(args[0]);
			else
				config = new XMLConfiguration("./conf/links.handler.xml");
		}
		catch(ConfigurationException ex) {
			return;
		}
		
		String redisHost = config.getString("redis.host", "localhost");
		String redisCollection = config.getString("redis.collection", "MediaItems");
		
		String mongoHost = config.getString("mongo.host", "localhost");
		String mongoDbName = config.getString("mongo.db", "Streams");
		String mongoCollection = config.getString("mongo.collection", "WebPages");
		String mediaCollection = config.getString("mongo.mediacollection", "MediaItems");
				
		DBObject query = new BasicDBObject("status", "new");
		
		URLExpanderBolt urlExpander;
		try {
			urlExpander = new URLExpanderBolt(mongoHost, mongoDbName, mongoCollection);
		} catch (Exception e) {
			return;
		}
		
		// Create topology 
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("injector", new MongoDbInjector(mongoHost, mongoDbName, mongoCollection, query), 1);
        
		builder.setBolt("ranker", new RankerBolt(), 1).shuffleGrouping("injector");
		builder.setBolt("expander", urlExpander, 8).shuffleGrouping("ranker");
		builder.setBolt("articleExtraction",  new ArticleExtractionBolt(48), 1).shuffleGrouping("expander", "article");
		builder.setBolt("mediaExtraction",  new MediaExtractionBolt(), 4).shuffleGrouping("expander", "media");
		
		UpdaterBolt updater = new UpdaterBolt(mongoHost, mongoDbName, mongoCollection, mediaCollection, redisHost, redisCollection);
		builder.setBolt("updater", updater , 4).shuffleGrouping("articleExtraction").shuffleGrouping("mediaExtraction");
		
        Config conf = new Config();
        conf.setDebug(false);
        
        try {
        	System.out.println("Submit topology to local cluster");
        	LocalCluster cluster = new LocalCluster();
        	cluster.submitTopology("twitter", conf, builder.createTopology());
        }
        catch(Exception e) {
        	e.printStackTrace();
        }
        
        /*
		while(true) {
			if(args!=null && (args.length == 1 || args.length == 2)) {
				int workers = 2;
				if(args.length>1) {
					try {
						workers = Integer.parseInt(args[1]);
					}
					catch(NumberFormatException e) {
						System.out.println(e.getMessage());
					}
				}
        	
				conf.setNumWorkers(workers);
				try {
					StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
				} catch (Exception e) {
					System.out.print(e.getMessage());
				}
			} else {
				System.out.println("Submit topology to local cluster");
				try {
					LocalCluster cluster = new LocalCluster();
					cluster.submitTopology("focused-crawler", conf, builder.createTopology());
				}
				catch(Exception e) {
					e.printStackTrace();
				}
			}
        
		}
		*/
	}
}