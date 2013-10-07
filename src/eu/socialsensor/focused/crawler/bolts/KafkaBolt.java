package eu.socialsensor.focused.crawler.bolts;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import eu.socialsensor.focused.crawler.utils.KafkaConsumer;
import eu.socialsensor.framework.common.domain.WebPage;
import eu.socialsensor.framework.common.factories.ObjectFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

public class KafkaBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	private static String TOPIC = "webpages";
	
	//private OutputCollector _collector;
	private KafkaConsumer _consumer;
	private Producer<String, String> _producer;

	
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		//this._collector = collector;
		
		Properties props = new Properties();
		
		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		
		ProducerConfig config = new ProducerConfig(props);
		this._producer = new Producer<String, String>(config);
		
		//TODO: Add arguments in constructor
		List<String> seedBrokers = new ArrayList<String>();
		seedBrokers.add("localhost");
		
		this._consumer = new KafkaConsumer(TOPIC, seedBrokers, 9092, 0);
	}

	@Override
	public void execute(Tuple input) {
		WebPage webPage = (WebPage) input.getValueByField("webPage");
		
		String url = webPage.getUrl();
		
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, url, webPage.toJSONString());
		_producer.send(data);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("webPage"));
	}
	
	class KafkaConsumtionThread extends Thread {

		
		public KafkaConsumtionThread() {	

		}

		public void run() {
			while(true) {
				List<String> msgs = _consumer.getMessages();
				if(msgs.size()>0) {
					for(String msg : msgs) {
						WebPage webPage = ObjectFactory.createWebPage(msg);
						System.out.println("KAFKA EMIT: " + webPage.toJSONString());
						//_collector.emit(tuple(webPage));
					}
				}
				else {
					Utils.sleep(500);
				}
			}
		};
	}

}
