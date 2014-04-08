package eu.socialsensor.focused.crawler.bolts.metrics;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class MetricsBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int c = 0;
	private long t = 0;
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		t = System.currentTimeMillis();
	}

	public void execute(Tuple input) {
		if(++c%100==0) {
			t = System.currentTimeMillis() - t;
			System.out.println(c + " tuples processed in " + t + " msecs");
			t = System.currentTimeMillis();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
