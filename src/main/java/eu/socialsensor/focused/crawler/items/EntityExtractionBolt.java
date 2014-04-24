package eu.socialsensor.focused.crawler.items;

import java.util.Map;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.Item;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class EntityExtractionBolt extends BaseRichBolt {

	private static final long serialVersionUID = 7935961067953158062L;

	private OutputCollector _collector;

	private Logger logger;

	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector = collector;
		this.logger = Logger.getLogger(EntityExtractionBolt.class);
	}

	public void execute(Tuple input) {
		try {
			Item item = (Item)input.getValueByField("Item");
			
			//TODO: Extract named entities in item's title and update the corresponding field
		
			_collector.emit(new Values(item));
		}
		catch(Exception e){
			logger.error(e);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Item"));
	}

}
