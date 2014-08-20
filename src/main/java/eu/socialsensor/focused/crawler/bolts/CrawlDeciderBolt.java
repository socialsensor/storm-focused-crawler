package eu.socialsensor.focused.crawler.bolts;

import java.util.Map;

import eu.socialsensor.framework.common.domain.MediaItem;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class CrawlDeciderBolt  extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8018495832386791557L;

	private String host;

	private JedisPool pool;
	private Jedis jedis;
	
	public CrawlDeciderBolt(String host) {
		this.host = host;
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		pool = new JedisPool(new JedisPoolConfig(), host);
		jedis = pool.getResource();
	}

	@Override
	public void execute(Tuple input) {
		
		MediaItem mItem = (MediaItem) input.getValueByField("MediaItem");
		
		String mId = mItem.getId();
		
		try {
			String value = jedis.get(mId);
			if(value == null) {
				
			}
			else {
				
			}
		}
		catch(Exception e) {
			
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	public static void main(String[] args) {
		
	}
	
}
