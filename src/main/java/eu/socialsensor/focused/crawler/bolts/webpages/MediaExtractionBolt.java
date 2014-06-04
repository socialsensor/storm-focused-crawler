package eu.socialsensor.focused.crawler.bolts.webpages;

import static backtype.storm.utils.Utils.tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.MediaItem;
import eu.socialsensor.framework.common.domain.StreamUser;
import eu.socialsensor.framework.common.domain.WebPage;
import eu.socialsensor.framework.retrievers.socialmedia.SocialMediaRetriever;
import eu.socialsensor.framework.retrievers.socialmedia.dailymotion.DailyMotionRetriever;
import eu.socialsensor.framework.retrievers.socialmedia.facebook.FacebookRetriever;
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
	
	private static final String SUCCESS = "success";
	private static final String FAILED = "failed";
	
	private static String MEDIA_STREAM = "media";
	private static String WEBPAGE_STREAM = "webpage";
	
	private Logger logger;
	
	private OutputCollector _collector;
	
	private static String instagramClientId = "50fca9eded824679934a71cfa8dda880";
	//private static String instagramToken = "";
	//private static String instagramSecret = "";
	
	private static String facebookToken = "260504214011769|jATWKceE7aVH4jxsB4DBuNjKBRc";
	
	private static String youtubeClientId = "manosetro";
	private static String youtubeDevKey = "AI39si6DMfJRhrIFvJRv0qFubHHQypIwjkD-W7tsjLJArVKn9iR-QoT8t-UijtITl4TuyHzK-cxqDDCkCBoJB-seakq1gbt1iQ";

	private static Pattern instagramPattern 	= 	Pattern.compile("https*://instagram.com/p/([\\w\\-]+)/");
	private static Pattern youtubePattern 		= 	Pattern.compile("https*://www.youtube.com/watch?.*v=([a-zA-Z0-9_\\-]+)(&.+=.+)*");
	private static Pattern vimeoPattern 		= 	Pattern.compile("https*://vimeo.com/([0-9]+)/*$");
	private static Pattern twitpicPattern 		= 	Pattern.compile("https*://twitpic.com/([A-Za-z0-9]+)/*.*$");
	private static Pattern dailymotionPattern 	= 	Pattern.compile("https*://www.dailymotion.com/video/([A-Za-z0-9]+)_.*$");
	private static Pattern facebookPattern 		= 	Pattern.compile("https*://www.facebook.com/photo.php?.*fbid=([a-zA-Z0-9_\\-]+)(&.+=.+)*");
	
	private Map<String, SocialMediaRetriever> retrievers = new HashMap<String, SocialMediaRetriever>();
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declareStream(MEDIA_STREAM, new Fields("MediaItem"));
    	declarer.declareStream(WEBPAGE_STREAM, new Fields("WebPage"));
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		this._collector = collector;  		
		logger = Logger.getLogger(MediaExtractionBolt.class);
		
		retrievers.put("instagram", new InstagramRetriever(instagramClientId));
		retrievers.put("youtube", new YoutubeRetriever(youtubeClientId, youtubeDevKey));
		retrievers.put("vimeo", new VimeoRetriever());
		retrievers.put("twitpic", new TwitpicRetriever());
		retrievers.put("dailymotion", new DailyMotionRetriever());
		retrievers.put("facebook", new FacebookRetriever(facebookToken));
	}

	public void execute(Tuple tuple) {
		
		WebPage webPage = (WebPage) tuple.getValueByField("webPage");
		
		if(webPage == null)
			return;
		
		String expandedUrl = webPage.getExpandedUrl();
		
		try {
			MediaItem mediaItem = getMediaItem(expandedUrl);	
			
			if(mediaItem != null) {
				webPage.setMedia(1);
				String[] mediaIds = {mediaItem.getId()};
				webPage.setMediaIds(mediaIds);
				mediaItem.setRef(webPage.getReference());
			}
			
			synchronized(_collector) {
				if(mediaItem != null) { 
					webPage.setStatus(SUCCESS);
					_collector.emit(WEBPAGE_STREAM, tuple(webPage));
					_collector.emit(MEDIA_STREAM, tuple(mediaItem));
				}
				else {
					logger.error(webPage.getExpandedUrl() + " failed due to null media item");
					webPage.setStatus(FAILED);
					_collector.emit(WEBPAGE_STREAM, tuple(webPage));
				}
			}
		} catch (Exception e) {
			logger.error(webPage.getExpandedUrl() + " failed due to exception");
			logger.error(e);
			synchronized(_collector) {
				webPage.setStatus(FAILED);
				_collector.emit(WEBPAGE_STREAM, tuple(webPage));
			}
		}

	}   
	
	private MediaItem getMediaItem(String url) {
		SocialMediaRetriever retriever = null;
		String mediaId = null;
		String source = null;
		
		Matcher matcher;
		if((matcher = instagramPattern.matcher(url)).matches()) {
			mediaId = matcher.group(1);
			retriever = retrievers.get("instagram");
			source = "instagram";
		}
		else if((matcher = youtubePattern.matcher(url)).matches()) {
			mediaId = matcher.group(1);
			retriever = retrievers.get("youtube");
			source = "youtube";
		}
		else if((matcher = vimeoPattern.matcher(url)).matches()){
			mediaId = matcher.group(1);
			retriever = retrievers.get("vimeo");
			source = "vimeo";
		}
		else if((matcher = twitpicPattern.matcher(url)).matches()) {
			mediaId = matcher.group(1);
			retriever = retrievers.get("twitpic");
			source = "twitpic";
		}
		else if((matcher = dailymotionPattern.matcher(url)).matches()) {
			mediaId = matcher.group(1);
			retriever = retrievers.get("dailymotion");
			source = "dailymotion";
		}
		else if((matcher = facebookPattern.matcher(url)).matches()) {
			mediaId = matcher.group(1);
			retriever = retrievers.get("facebook");
			source = "facebook";
		}
		else {
			logger.error(url + " matches nothing.");
			return null;
		}
		
		if(mediaId == null || retriever == null) {
			return null;
		}
		
		try {
			MediaItem mediaItem = retriever.getMediaItem(mediaId);
			if(mediaItem == null) {
				logger.info(mediaId + " from " + source + " is null");
				return null;
			}
			
			mediaItem.setPageUrl(url);
			
			StreamUser streamUser = mediaItem.getUser();
			String userid = mediaItem.getUserId();
			if(streamUser == null || userid == null) {
				streamUser = retriever.getStreamUser(userid);
				if(streamUser == null) {
					throw new Exception("Missing " + mediaItem.getStreamId() + " user: " + userid);
				}
				mediaItem.setUser(streamUser);
				mediaItem.setUserId(streamUser.getId());
			}
			
			return mediaItem;
		}
		catch(Exception e) {
			logger.error(e);
			return null;
		}
	}

	@Override
	public void cleanup() {
		
	}
}