package eu.socialsensor.focused.crawler.spouts;

import static backtype.storm.utils.Utils.tuple;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import eu.socialsensor.framework.common.domain.WebPage;
import eu.socialsensor.framework.common.factories.ObjectFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class RedisInjector extends BaseRichSpout {

	static final long serialVersionUID = 737015318988609460L;

	static final String webPagesChannel = "WebPages";
	
	SpoutOutputCollector _collector;
	final String host;
	
	LinkedBlockingQueue<String> queue;
	JedisPool pool;

	public RedisInjector(String host) {
		this.host = host;
	}

	class ListenerThread extends Thread {
		LinkedBlockingQueue<String> queue;
		JedisPool pool;
		String pattern;

		public ListenerThread(LinkedBlockingQueue<String> queue, JedisPool pool) {
			this.queue = queue;
			this.pool = pool;
		}

		public void run() {

			JedisPubSub listener = new JedisPubSub() {

				@Override
				public void onMessage(String channel, String message) {
					queue.offer(message);
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
				jedis.subscribe(listener, webPagesChannel);
			} finally {
				pool.returnResource(jedis);
			}
		}
	};

	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		queue = new LinkedBlockingQueue<String>(5000);
		pool = new JedisPool(new JedisPoolConfig(),host);

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
        	try {
        		WebPage webPage = ObjectFactory.createWebPage(ret);
            	_collector.emit(tuple(webPage));    
        	}
        	catch(Exception e) {
        		
        	}
        }
	}

	public void ack(Object msgId) {

	}

	public void fail(Object msgId) {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("webPage"));
	}

	public boolean isDistributed() {
		return false;
	}
}