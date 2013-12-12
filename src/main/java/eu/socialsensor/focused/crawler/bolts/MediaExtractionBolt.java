package eu.socialsensor.focused.crawler.bolts;

import static backtype.storm.utils.Utils.tuple;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eu.socialsensor.framework.common.domain.MediaItem;
import eu.socialsensor.framework.common.domain.WebPage;
import eu.socialsensor.framework.retrievers.MediaRetriever;
import eu.socialsensor.framework.retrievers.dailymotion.DailyMotionMediaRetriever;
import eu.socialsensor.framework.retrievers.instagram.InstagramMediaRetriever;
import eu.socialsensor.framework.retrievers.twitpic.TwitpicMediaRetriever;
import eu.socialsensor.framework.retrievers.vimeo.VimeoMediaRetriever;
import eu.socialsensor.framework.retrievers.youtube.YtMediaRetriever;

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
	
	private OutputCollector _collector;
	
	private static String instagramToken = "342704836.5b9e1e6.503a35185da54224adaa76161a573e71";
	private static String instagramSecret = "e53597da6d7749d2a944651bbe6e6f2a";
	
	private static String youtubeClientId = "manosetro";
	private static String youtubeDevKey = "AI39si6DMfJRhrIFvJRv0qFubHHQypIwjkD-W7tsjLJArVKn9iR-QoT8t-UijtITl4TuyHzK-cxqDDCkCBoJB-seakq1gbt1iQ";
	
	
	private static Pattern instagramPattern = Pattern.compile("http://instagram.com/p/([\\w\\-]+)/");
	private static Pattern youtubePattern = Pattern.compile("http://www.youtube.com/watch?.*v=([a-zA-Z0-9_]+)(&.+=.+)*");
	private static Pattern vimeoPattern = Pattern.compile("http://vimeo.com/([0-9]+)/*$");
	private static Pattern twitpicPattern = Pattern.compile("http://twitpic.com/([A-Za-z0-9]+)/*.*$");
	private static Pattern dailymotionPattern = Pattern.compile("http://www.dailymotion.com/video/([A-Za-z0-9]+)_.*$");
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("url", "expandedUrl", "type", "content"));
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		this._collector = collector;  		
	}

	public void execute(Tuple tuple) {
		
		WebPage webPage = (WebPage) tuple.getValueByField("webPage");
		
		if(webPage == null)
			return;
		
		String url = webPage.getUrl();
		String expandedUrl = webPage.getExpandedUrl();

		try {
			MediaItem mediaItem = getMediaItem(expandedUrl);	
			
			if(mediaItem != null) {
				mediaItem.setRef(webPage.getReference());
			}
			
			synchronized(_collector) {
				if(mediaItem != null) { 
					mediaItem.setRefUrl(url);
					mediaItem.setPageUrl(expandedUrl);
					_collector.emit(tuple(url, expandedUrl, "media", mediaItem));
				}
				else 
				_collector.emit(tuple(url, expandedUrl, "exception", "Cannot find any media item"));
			
			}
		} catch (Exception e) {
			synchronized(_collector) {
				_collector.emit(tuple(url, expandedUrl, "exception", e.getMessage()));
			}
		}

	}   
	
	private MediaItem getMediaItem(String url) throws Exception {
		MediaRetriever retriever = null;
		String mediaId = null;
		
		Matcher matcher;
		if((matcher = instagramPattern.matcher(url)).matches()) {
			mediaId = matcher.group(1);
			retriever = new InstagramMediaRetriever(instagramSecret, instagramToken);
		}
		else if((matcher = youtubePattern.matcher(url)).matches()) {
			mediaId = matcher.group(1);
			retriever = new YtMediaRetriever(youtubeClientId, youtubeDevKey);
		}
		else if((matcher = vimeoPattern.matcher(url)).matches()){
			mediaId = matcher.group(1);
			retriever = new VimeoMediaRetriever();
		}
		else if((matcher = twitpicPattern.matcher(url)).matches()) {
			mediaId = matcher.group(1);
			retriever = new TwitpicMediaRetriever();
		}
		else if((matcher = dailymotionPattern.matcher(url)).matches()) {
			mediaId = matcher.group(1);
			retriever = new DailyMotionMediaRetriever();
		}
		else {
			return null;
		}
		
		if(mediaId == null)
			return null;
		
		MediaItem mediaItem = retriever.getMediaItem(mediaId);
		if(mediaItem == null) {
			throw new Exception();
		}
		return mediaItem;
	
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
}