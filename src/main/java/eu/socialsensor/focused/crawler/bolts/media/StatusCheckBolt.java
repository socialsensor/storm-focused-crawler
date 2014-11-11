package eu.socialsensor.focused.crawler.bolts.media;

import static backtype.storm.utils.Utils.tuple;

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
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class StatusCheckBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -9197307413110165210L;
	
	private Logger _logger;
	
	private String _host;
	private JedisPool _pool;

	private OutputCollector collector;

	public StatusCheckBolt(String host) {
		_host = host;
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
	
		_logger = Logger.getLogger(MediaUpdaterBolt.class);
		
		_pool = new JedisPool(new JedisPoolConfig(), _host);
		
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		
		try {
			MediaItem mediaItem = (MediaItem) tuple.getValueByField("MediaItem");
			if(mediaItem == null) {
				return;
			}
			
			Jedis jedis = _pool.getResource();
		
			String mId = mediaItem.getId();
			String value = jedis.hget(mId, "STATUS");
			
			if(value == null) {
				collector.emit(tuple(mediaItem));
				jedis.hset(mId, "STATUS", "INJECTED");
			}
		
			_pool.returnResource(jedis);
		}
		catch(Exception e) {
			_logger.error(e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("MediaItem"));
	}

}
