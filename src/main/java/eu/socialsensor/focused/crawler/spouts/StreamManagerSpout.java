package eu.socialsensor.focused.crawler.spouts;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import eu.socialsensor.framework.streams.StreamException;
import eu.socialsensor.sfc.builder.InputConfiguration;
import eu.socialsensor.sfc.streams.StreamsManagerConfiguration;
import eu.socialsensor.sfc.streams.management.StreamsManager;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

public class StreamManagerSpout  extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5824716039413055289L;
	private StreamsManagerConfiguration config;
	private InputConfiguration inputConfig;

	public StreamManagerSpout(String streamConfig, String inputConfig) throws ParserConfigurationException, SAXException, IOException {
		File streamConfigFile = new File(streamConfig);
		File inputConfigFile = new File(inputConfig);
		
		this.config = StreamsManagerConfiguration.readFromFile(streamConfigFile);		
		this.inputConfig = InputConfiguration.readFromFile(inputConfigFile);		
	}
	
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		StreamsManager manager;
		try {
			manager = new StreamsManager(config, inputConfig);
			manager.open();
			
		} catch (StreamException e) {
			e.printStackTrace();
		}
		
	}

	public void nextTuple() {
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {	
		
	}

}
