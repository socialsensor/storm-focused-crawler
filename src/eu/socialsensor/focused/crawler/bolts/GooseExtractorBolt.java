package eu.socialsensor.focused.crawler.bolts;

import static backtype.storm.utils.Utils.tuple;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.gravity.goose.Configuration;
import com.gravity.goose.Goose;

import eu.socialsensor.focused.crawler.models.Article;
import eu.socialsensor.framework.common.domain.MediaItem;



public class GooseExtractorBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private Goose goose;

	private OutputCollector _collector;

	private int minDim = 150;
	private int minArea = 200 * 200;
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		this._collector = collector;
		
		Configuration config = new Configuration();
		config.enableImageFetching_$eq(false); 
		goose = new Goose(config);		
	}

	@Override
	public void execute(Tuple input) {
		String url = input.getStringByField("url");
		String expandedUrl = input.getStringByField("expandedUrl");
		
		try {
			com.gravity.goose.Article gooseArticle = goose.extractContent(expandedUrl);

			
			gooseArticle.metaDescription();
			if(gooseArticle.topImage() != null && gooseArticle.topImage().imageSrc().length() != 0) {
				gooseArticle.topImage().imageSrc();
			}
			
			String title = gooseArticle.title();
			String text = gooseArticle.cleanedArticleText();
			
			List<MediaItem> mediaItems = getImages(gooseArticle, "");
			
			Article article = new Article(title, text);
			
			for(MediaItem mediaItem : mediaItems) 
				article.addMediaItem(mediaItem);
			
			synchronized(_collector) {
				_collector.emit(tuple(url, expandedUrl, "goose", article));
			}
		} catch(Exception e) {
			_collector.emit(tuple(url, expandedUrl, "exception", e.getMessage()));
		}
		
	}

	private List<MediaItem> getImages(com.gravity.goose.Article article, String ref) {
		List<MediaItem> mediaItems = new ArrayList<MediaItem>();
		Document doc = article.rawDoc();
		
		String base = article.finalUrl();
		int pageHash = (base.hashCode() & 0x7FFFFFFF);
		
		Elements imageElements = doc.getElementsByTag("img");
		Iterator<Element> it = imageElements.iterator();
		while(it.hasNext()) {
			Element image = it.next();
			String height = image.attr("height");
			String width = image.attr("width");
			
			if(height==null || width==null || height.equals("") || width.equals(""))
				continue;
			
			int w = Integer.parseInt(width);
			int h = Integer.parseInt(height);
			
			if(w*h<minArea || w<minDim || h<minDim)
				continue;
			
			String title = image.attr("alt");
			String imageUrl = image.attr("src");

			URL url;
			try {
				url = new URL(imageUrl);
				
				if(imageUrl.endsWith(".gif") || url.getPath().endsWith(".gif"))
					continue;
				
				MediaItem mi = new MediaItem(url);
				
				// Create image unique id
				int imageHash = (url.hashCode() & 0x7FFFFFFF);	
				mi.setId("Web::"+pageHash+"_"+imageHash);
				
				mi.setPageUrl(base);
				mi.setRef(ref);
				
				mi.setTitle(title);
				mi.setType("image");
				
				mi.setSize(w, h);
			} catch (MalformedURLException e) {
				continue;
			}
			
		}
		return mediaItems;
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url", "expandedUrl", "type", "content"));
	}

}    