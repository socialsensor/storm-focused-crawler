package eu.socialsensor.focused.crawler.bolts.webpages;

import static backtype.storm.utils.Utils.tuple;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.MediaItem;
import eu.socialsensor.framework.common.domain.WebPage;
import eu.socialsensor.framework.retrievers.socialmedia.SocialMediaRetriever;
import eu.socialsensor.framework.retrievers.socialmedia.dailymotion.DailyMotionRetriever;
import eu.socialsensor.framework.retrievers.socialmedia.instagram.InstagramRetriever;
import eu.socialsensor.framework.retrievers.socialmedia.twitpic.TwitpicRetriever;
import eu.socialsensor.framework.retrievers.socialmedia.vimeo.VimeoRetriever;
import eu.socialsensor.framework.retrievers.socialmedia.youtube.YoutubeRetriever;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;


public class MediaExtractionBolt extends BaseRichBolt {

    /**
	 * 
	 */
	private static final long serialVersionUID = -2548434425109192911L;
	
	private Logger logger;
	
	private OutputCollector _collector;
	
	private static String instagramToken = "";
	private static String instagramSecret = "";
	
	private static String youtubeClientId = "";
	private static String youtubeDevKey = "";
	
	
	private static Pattern instagramPattern = Pattern.compile("http://instagram.com/p/([\\w\\-]+)/");
	private static Pattern youtubePattern = Pattern.compile("http://www.youtube.com/watch?.*v=([a-zA-Z0-9_]+)(&.+=.+)*");
	private static Pattern vimeoPattern = Pattern.compile("http://vimeo.com/([0-9]+)/*$");
	private static Pattern twitpicPattern = Pattern.compile("http://twitpic.com/([A-Za-z0-9]+)/*.*$");
	private static Pattern dailymotionPattern = Pattern.compile("http://www.dailymotion.com/video/([A-Za-z0-9]+)_.*$");
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("url", "expandedUrl", "domain", "type", "content"));
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		this._collector = collector;  		
		logger = Logger.getLogger(MediaExtractionBolt.class);
	}

	public void execute(Tuple tuple) {
		
		WebPage webPage = (WebPage) tuple.getValueByField("webPage");
		
		if(webPage == null)
			return;
		
		String url = webPage.getUrl();
		String expandedUrl = webPage.getExpandedUrl();
		String domain = webPage.getDomain();
		
		try {
			MediaItem mediaItem = getMediaItem(expandedUrl);	
			
			if(mediaItem != null) {
				mediaItem.setRef(webPage.getReference());
			}
			
			synchronized(_collector) {
				if(mediaItem != null) { 
					mediaItem.setPageUrl(expandedUrl);
					_collector.emit(tuple(url, expandedUrl, domain, "media", mediaItem));
				}
				else {
					_collector.emit(tuple(url, expandedUrl, domain, "exception", "Cannot find any media item"));
				}
			}
		} catch (Exception e) {
			logger.error(e);
			synchronized(_collector) {
				_collector.emit(tuple(url, expandedUrl, domain, "exception", e.getMessage()));
			}
		}

	}   
	
	private MediaItem getMediaItem(String url) throws Exception {
		SocialMediaRetriever retriever = null;
		String mediaId = null;
		
		Matcher matcher;
		if((matcher = instagramPattern.matcher(url)).matches()) {
			mediaId = matcher.group(1);
			retriever = new InstagramRetriever(instagramSecret, instagramToken);
		}
		else if((matcher = youtubePattern.matcher(url)).matches()) {
			mediaId = matcher.group(1);
			retriever = new YoutubeRetriever(youtubeClientId, youtubeDevKey);
		}
		else if((matcher = vimeoPattern.matcher(url)).matches()){
			mediaId = matcher.group(1);
			retriever = new VimeoRetriever();
		}
		else if((matcher = twitpicPattern.matcher(url)).matches()) {
			mediaId = matcher.group(1);
			retriever = new TwitpicRetriever();
		}
		else if((matcher = dailymotionPattern.matcher(url)).matches()) {
			mediaId = matcher.group(1);
			retriever = new DailyMotionRetriever();
		}
		else {
			return null;
		}
		
		if(mediaId == null)
			return null;
		
		MediaItem mediaItem = retriever.getMediaItem(mediaId);
		if(mediaItem == null) {
			throw new Exception("Media item is null!");
		}
		
		//TODO: Check missing user 
		
		return mediaItem;
	
	}

	@Override
	public void cleanup() {
		
	}
}