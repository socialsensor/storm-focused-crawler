package eu.socialsensor.focused.crawler.bolts.webpages;

import static backtype.storm.utils.Utils.tuple;

import java.util.Map;
import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.WebPage;
import eu.socialsensor.framework.common.factories.ItemFactory;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class WebPageDeserializationBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Logger _logger;
	
	private OutputCollector _collector;

	private String inputField;

	public WebPageDeserializationBolt(String inputField) {
		this.inputField = inputField;
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector = collector;	
		_logger = Logger.getLogger(WebPageDeserializationBolt.class);

	}

	public void execute(Tuple input) {
		try {
			String json = input.getStringByField(inputField);
			WebPage webPage = ItemFactory.createWebPage(json);
			if(webPage != null) {
				_collector.emit(tuple(webPage));
			}
		} catch(Exception e) {
			_logger.error("Exception: "+e.getMessage());
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(inputField));
	}

}
