package eu.socialsensor.focused.crawler.bolts.webpages;

import java.util.Map;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.MediaItem;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class RedisBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private Jedis publisherJedis;
	private String host, channel;

	private Logger logger;
	
	public RedisBolt(String host, String channel) {
		this.host = host;
		this.channel = channel;
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {

		JedisPoolConfig poolConfig = new JedisPoolConfig();
        JedisPool jedisPool = new JedisPool(poolConfig, host, 6379, 0);
		
        publisherJedis = jedisPool.getResource();
        logger = Logger.getLogger(RedisBolt.class);
	}

	public void execute(Tuple input) {
		try {
			MediaItem mi = (MediaItem) input.getValueByField("MediaItem");
			if(mi != null) {
				publisherJedis.publish(channel, mi.toJSONString());
			}
		}
		catch(Exception e) {
			logger.error(e);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
