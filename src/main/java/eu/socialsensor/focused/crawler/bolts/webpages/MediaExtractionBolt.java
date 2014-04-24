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
	
	private static String instagramToken = "";
	private static String instagramSecret = "";
	
	private static String youtubeClientId = "";
	private static String youtubeDevKey = "";
	
	private static Pattern instagramPattern 	= 	Pattern.compile("http://instagram.com/p/([\\w\\-]+)/");
	private static Pattern youtubePattern 		= 	Pattern.compile("http://www.youtube.com/watch?.*v=([a-zA-Z0-9_]+)(&.+=.+)*");
	private static Pattern vimeoPattern 		= 	Pattern.compile("http://vimeo.com/([0-9]+)/*$");
	private static Pattern twitpicPattern 		= 	Pattern.compile("http://twitpic.com/([A-Za-z0-9]+)/*.*$");
	private static Pattern dailymotionPattern 	= 	Pattern.compile("http://www.dailymotion.com/video/([A-Za-z0-9]+)_.*$");
	
	private Map<String, SocialMediaRetriever> retrievers = new HashMap<String, SocialMediaRetriever>();
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declareStream(MEDIA_STREAM, new Fields("MediaItem"));
    	declarer.declareStream(WEBPAGE_STREAM, new Fields("WebPage"));
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		this._collector = collector;  		
		logger = Logger.getLogger(MediaExtractionBolt.class);
		
		retrievers.put("instagram", new InstagramRetriever(instagramSecret, instagramToken));
		retrievers.put("youtube", new YoutubeRetriever(youtubeClientId, youtubeDevKey));
		retrievers.put("vimeo", new VimeoRetriever());
		retrievers.put("twitpic", new TwitpicRetriever());
		retrievers.put("dailymotion", new DailyMotionRetriever());
		
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
					webPage.setStatus(FAILED);
					_collector.emit(WEBPAGE_STREAM, tuple(webPage));
				}
			}
		} catch (Exception e) {
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
		
		Matcher matcher;
		if((matcher = instagramPattern.matcher(url)).matches()) {
			mediaId = matcher.group(1);
			//retriever = new InstagramRetriever(instagramSecret, instagramToken);
			retriever = retrievers.get("instagram");
		}
		else if((matcher = youtubePattern.matcher(url)).matches()) {
			mediaId = matcher.group(1);
			//retriever = new YoutubeRetriever(youtubeClientId, youtubeDevKey);
			retriever = retrievers.get("youtube");
		}
		else if((matcher = vimeoPattern.matcher(url)).matches()){
			mediaId = matcher.group(1);
			//retriever = new VimeoRetriever();
			retriever = retrievers.get("vimeo");
		}
		else if((matcher = twitpicPattern.matcher(url)).matches()) {
			mediaId = matcher.group(1);
			//retriever = new TwitpicRetriever();
			retriever = retrievers.get("twitpic");
		}
		else if((matcher = dailymotionPattern.matcher(url)).matches()) {
			mediaId = matcher.group(1);
			//retriever = new DailyMotionRetriever();
			retriever = retrievers.get("dailymotion");
		}
		else {
			return null;
		}
		
		if(mediaId == null || retriever == null)
			return null;
		
		try {
			MediaItem mediaItem = retriever.getMediaItem(mediaId);
			if(mediaItem != null) {
				mediaItem.setPageUrl(url);
			}
			StreamUser streamUser = mediaItem.getUser();
			String userid = mediaItem.getUserId();
			if(streamUser == null || userid == null) {
				streamUser = retriever.getStreamUser(userid);
				if(streamUser == null) {
					throw new Exception("Missing " + mediaItem.getStreamId() + " user: " + userid);
				}
				mediaItem.setUser(streamUser);
				mediaItem.setUserId(streamUser.getUserid());
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