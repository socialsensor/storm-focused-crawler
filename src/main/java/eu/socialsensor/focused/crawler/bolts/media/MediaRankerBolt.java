package eu.socialsensor.focused.crawler.bolts.media;

import java.util.Map;

import eu.socialsensor.framework.common.domain.MediaItem;
import eu.socialsensor.framework.common.factories.ObjectFactory;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class MediaRankerBolt extends BaseRichBolt {

    /**
	 * 
	 */
	private static final long serialVersionUID = -2548434425109192911L;
	private OutputCollector _collector;
	private String inputField;

	public MediaRankerBolt(String inputField) {
		this.inputField = inputField;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("id", "url", "score", "size"));
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		this._collector = collector;
	}

	public void execute(Tuple tuple) {
		String json = tuple.getStringByField(inputField);
		MediaItem mediaItem = ObjectFactory.createMediaItem(json);
		
		String id = mediaItem.getId();
		String url = mediaItem.getUrl();
		Long shares = mediaItem.getShares();
		
		boolean size = true;
		
		if(mediaItem.getWidth() == null || mediaItem.getHeight() == null)
				size  = false;
		
		double sharesScore = 1 - Math.exp(-0.05 * shares);
		sharesScore = (sharesScore + 1) / 2;
		
		_collector.emit(new Values(id, url, sharesScore, size));
        _collector.ack(tuple);
        
	}   
}