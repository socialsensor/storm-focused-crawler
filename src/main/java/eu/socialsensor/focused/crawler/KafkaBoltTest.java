package eu.socialsensor.focused.crawler;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import eu.socialsensor.focused.crawler.bolts.KafkaBolt;
import eu.socialsensor.focused.crawler.bolts.RankerBolt;
import eu.socialsensor.focused.crawler.spouts.MongoDbInjector;

public class KafkaBoltTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String mongoHost = "160.40.50.207";
		String mongoDbName = "FeteBerlin"; 
		String mongoCollection = "WebPages";
		
		DBObject query = new BasicDBObject();
	
		
		// Create topology 
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("injector", new MongoDbInjector(mongoHost, mongoDbName, mongoCollection, query), 1);
		builder.setBolt("ranker", new RankerBolt(), 1).shuffleGrouping("injector");	        
		builder.setBolt("kafka",  new KafkaBolt(), 1).shuffleGrouping("ranker");

		
		Config conf = new Config();
		conf.setDebug(false);

		System.out.println("Submit topology to local cluster");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("twitter", conf, builder.createTopology());
        
	}

}
