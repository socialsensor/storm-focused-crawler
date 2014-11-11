package eu.socialsensor.focused.crawler.bolts.media;

import static backtype.storm.utils.Utils.tuple;

import java.util.Map;
import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.MediaItem;
import eu.socialsensor.framework.common.factories.ItemFactory;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class MediaItemDeserializationBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Logger _logger;
	
	private OutputCollector _collector;

	private String inputField;

	public MediaItemDeserializationBolt(String inputField) {
		this.inputField = inputField;
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;

		_logger = Logger.getLogger(MediaItemDeserializationBolt.class);
		
	}

	public void execute(Tuple input) {
		try {
			String json = input.getStringByField(inputField);
			MediaItem mediaItem = ItemFactory.createMediaItem(json);
			if(mediaItem != null) {
				_collector.emit(tuple(mediaItem));
			}
		} catch(Exception e) {
				_logger.error("Exception: "+e.getMessage());
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("MediaItem"));
	}
	
}
