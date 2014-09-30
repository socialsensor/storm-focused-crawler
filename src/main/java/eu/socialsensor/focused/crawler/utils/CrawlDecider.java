package eu.socialsensor.focused.crawler.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class CrawlDecider {

	private String host;

	private JedisPool pool;
	private Jedis jedis;
	
	private static enum Status {
		NEW, INJECTED, FAILED, SUCCESSFULL
	}
	
	private static Integer expirationTime = 24 * 3600;
	
	public CrawlDecider(String redisHost) {
		host = redisHost;
		pool = new JedisPool(new JedisPoolConfig(), host);
		jedis = pool.getResource();

	}

	private void setStatus(String id, String status) {
		jedis.hset(id, "status", status);
		jedis.expire(id, expirationTime);
		
	}
	
	private String getStatus(String id) {
		String value = jedis.hget(id, "status");
		return value;
	}
	
	public void deleteStatus(String id) {
		jedis.del(id);
	}
	
	public static void main(String[] args) {
		CrawlDecider decider = new CrawlDecider("160.40.50.207");
		//decider.deleteStatus("1");
		
		//decider.setStatus("1", "312");
		
		System.out.println(decider.getStatus("1"));
	}
	
}
