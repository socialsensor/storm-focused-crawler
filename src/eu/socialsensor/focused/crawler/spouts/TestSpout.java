package eu.socialsensor.focused.crawler.spouts;

import static backtype.storm.utils.Utils.tuple;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class TestSpout extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private SpoutOutputCollector _collector;
	private Random _RND;
	
	List<String> urls = new ArrayList<String>();
	
	Set<String> targets = new HashSet<String>();
	
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		_collector = collector;
		_RND = new Random();
		
		urls.add("http://www.youtube.com/watch?v=rH4Pf9M9o4o&list=PL840550EE4F5381AA");
		urls.add("http://money.cnn.com/2013/08/20/technology/social/facebook-zuckerberg-5-billion/index.html?hpt=hp_c1");
		urls.add("http://instagram.com/p/dPajryS9b6/");
		urls.add("http://www.youtube.com/watch?v=hfFqQQuuEa8");
		urls.add("http://www.bbc.co.uk/news/world-asia-23776345");
		urls.add("http://instagram.com/p/dNipWcj7bU/");
		urls.add("http://www.theonion.com/articles/obama-family-adopts-44yearold-portuguese-water-man,33558/");
		urls.add("http://www.thedrum.com/news/2013/07/25/spanish-security-video-footage-circulating-youtube-shows-terrifying-train-crash");
		urls.add("http://vimeo.com/71271260");
		urls.add("http://twitpic.com/d9gch2");
	
		targets.add("vimeo.com");
		targets.add("instagram.com");
		targets.add("www.youtube.com");
		targets.add("twitpic.com");
		
	}

	@Override
	public void nextTuple() {
		Utils.sleep(1000);
		int index = _RND.nextInt(urls.size());
		
		String urlStr = urls.get(index);
		URL url = null;
		try {
			url = new URL(urlStr);
			String domain = url.getHost();
			
			String streamId = "url";
			if(targets.contains(domain)) {
				streamId  = "api";
			}
			_collector.emit(streamId, tuple(urlStr, domain));
			
		} catch (MalformedURLException e) { }
		
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("url", new Fields("url", "domain"));
		declarer.declareStream("api", new Fields("url", "domain"));

	}

}
