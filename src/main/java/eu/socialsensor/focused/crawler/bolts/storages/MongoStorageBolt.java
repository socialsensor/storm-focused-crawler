package eu.socialsensor.focused.crawler.bolts.storages;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class MongoStorageBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2613697672344106360L;

	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
	}

	public void execute(Tuple input) {
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) { }

}
