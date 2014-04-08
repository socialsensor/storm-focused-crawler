package eu.socialsensor.focused.crawler;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import eu.socialsensor.focused.crawler.bolts.media.MediaRankerBolt;
import eu.socialsensor.focused.crawler.bolts.media.MediaUpdaterBolt;
import eu.socialsensor.focused.crawler.bolts.media.VisualIndexerBolt;
import eu.socialsensor.focused.crawler.spouts.MongoDbSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class StormVisualIndexer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		XMLConfiguration config;
		try {
			if(args.length == 1)
				config = new XMLConfiguration(args[0]);
			else
				config = new XMLConfiguration("./conf/visual.indexer.xml");
		}
		catch(ConfigurationException ex) {
			return;
		}
		
		//String redisHost = config.getString("redis.host");
		
		String mongoHost = config.getString("mongodb.host");
		String mongoDbName = config.getString("mongodb.db");
		String mongoCollectionName = config.getString("mongodb.collection");
		String clustersCollectionName = config.getString("mongodb.clusters");
		
		String indexHostname = config.getString("visualindex.host");
		String indexCollection = config.getString("visualindex.collection");
		
		String learningFiles = config.getString("learningfiles");
		if(!learningFiles.endsWith("/"))
			learningFiles = learningFiles + "/";
		
		String[] codebookFiles = { 
				learningFiles + "surf_l2_128c_0.csv",
				learningFiles + "surf_l2_128c_1.csv", 
				learningFiles + "surf_l2_128c_2.csv",
				learningFiles + "surf_l2_128c_3.csv" };
		
		String pcaFile = learningFiles + "pca_surf_4x128_32768to1024.txt";
	
		VisualIndexerBolt visualIndexer;
		try {
			visualIndexer = new VisualIndexerBolt(indexHostname, indexCollection, codebookFiles, pcaFile);
		} catch (Exception e) {
			return;
		}
		
		DBObject query = new BasicDBObject("type", "image");
		query.put("vIndexed", false);
		query.put("status", "new");
		
		MediaUpdaterBolt updater = new MediaUpdaterBolt(mongoHost, mongoDbName, mongoCollectionName, clustersCollectionName,
				indexHostname, indexCollection);
		
		TopologyBuilder builder = new TopologyBuilder();
        
		builder.setSpout("injector", new MongoDbSpout(mongoHost, mongoDbName, mongoCollectionName, query), 1);
        //builder.setSpout("injector", new RedisInjector(redisHost), 1);
        builder.setBolt("ranker", new MediaRankerBolt("MediaItems"), 4).shuffleGrouping("injector");
        builder.setBolt("indexer", visualIndexer, 16).shuffleGrouping("ranker");
     
		builder.setBolt("updater", updater, 1).shuffleGrouping("indexer");
        
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        
        cluster.submitTopology("visual-indexer", conf, builder.createTopology());
        
	}

}
