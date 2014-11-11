package eu.socialsensor.focused.crawler.spouts;

import static backtype.storm.utils.Utils.tuple;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class RedisSpout extends BaseRichSpout {

	private Logger logger;
	
	static final long serialVersionUID = 737015318988609460L;

	private String channel;
	
	private SpoutOutputCollector _collector;
	private final String host;
	
	private LinkedBlockingQueue<String> queue;
	private JedisPool pool;

	private String uniqueField = null;

	public RedisSpout(String host, String channel) {
		this.host = host;
		this.channel = channel;
	}
	
	public RedisSpout(String host, String channel, String uniqueField) {
		this(host, channel);
		this.uniqueField = uniqueField;
	}

	class ListenerThread extends Thread {
		
		private LinkedBlockingQueue<String> queue;
		private JedisPool pool;
		
		private int totalMessages = 0;
		private Set<String> ids = new HashSet<String>();
			
		public ListenerThread(LinkedBlockingQueue<String> queue, JedisPool pool) {
			this.queue = queue;
			this.pool = pool;
		}

		public void run() {
			
			JedisPubSub listener = new JedisPubSub() {

				@Override
				public void onMessage(String channel, String message) {
					totalMessages++;
				
					DBObject obj = (DBObject) JSON.parse(message);
					
					if(uniqueField != null) {
						// Emit only unique messages based on uniqueField
						String id = (String) obj.get(uniqueField);
						if(!ids.contains(id)) {
							queue.offer(message);
							ids.add(id);
							if(ids.size() % 2000 == 0) {
								logger.info(totalMessages + " messages received in total from " + channel + ". "  
									+ ids.size() + " unique. " 
									+ queue.size() + " in redis spout queue.");
							}
						}
					}
					else {
						// Emit all messages
						queue.offer(message);
					}
					
				}

				@Override
				public void onPMessage(String pattern, String channel, String message) { }

				@Override
				public void onPSubscribe(String channel, int subscribedChannels) { }

				@Override
				public void onPUnsubscribe(String channel, int subscribedChannels) { }

				@Override
				public void onSubscribe(String channel, int subscribedChannels) { }

				@Override
				public void onUnsubscribe(String channel, int subscribedChannels) { }
			
			};

			Jedis jedis = pool.getResource();
			try {
				logger.info("Subscribe on " + channel);
				
				jedis.subscribe(listener, channel);
			} finally {
				pool.returnResource(jedis);
			}
		}
	};

	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {

		logger = Logger.getLogger(RedisSpout.class);
		
		_collector = collector;
		queue = new LinkedBlockingQueue<String>(10000);
		pool = new JedisPool(new JedisPoolConfig(), host);
		
		ListenerThread listener = new ListenerThread(queue, pool);
		listener.start();

		
	}

	public void close() {
		pool.destroy();
	}

	public void nextTuple() {
		String ret = queue.poll();
        if(ret == null) {
            Utils.sleep(50);
        } else {
            _collector.emit(tuple(ret));            
        }
	}

	public void ack(Object msgId) {

	}

	public void fail(Object msgId) {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(channel));
	}

	public boolean isDistributed() {
		return false;
	}
	
	public static void main(String...args) {
		
		/* */
		RedisSpout spout = new RedisSpout("xxx.xxx.xxx.xxx", "media", "url");
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("injector", spout, 1);
		
		Config conf = new Config();
        conf.setDebug(false);

		System.out.println("Run topology in local mode");
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("redis-spout-test", conf, builder.createTopology());
		
	}
	
	
}