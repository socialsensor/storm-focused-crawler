package eu.socialsensor.focused.crawler.bolts;

import static backtype.storm.utils.Utils.tuple;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import org.xml.sax.InputSource;

import de.l3s.boilerpipe.BoilerpipeExtractor;
import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.document.Image;
import de.l3s.boilerpipe.document.TextDocument;
import de.l3s.boilerpipe.document.TextDocumentStatistics;
import de.l3s.boilerpipe.estimators.SimpleEstimator;
import de.l3s.boilerpipe.extractors.CommonExtractors;
import de.l3s.boilerpipe.sax.BoilerpipeSAXInput;
import de.l3s.boilerpipe.sax.ImageExtractor;
import eu.socialsensor.focused.crawler.models.Article;
import eu.socialsensor.framework.common.domain.MediaItem;
import eu.socialsensor.framework.common.domain.WebPage;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;


public class ArticleExtractionBolt extends BaseRichBolt {

    /**
	 * 
	 */
	private static final long serialVersionUID = -2548434425109192911L;
	private OutputCollector _collector;
	private DefaultHttpClient _httpclient;
	
	private BoilerpipeExtractor _extractor;
	private BoilerpipeExtractor _articleExtractor;
	private ImageExtractor _imageExtractor;
	private SimpleEstimator _estimator;
	
	private int minDim = 150;
	private int minArea = 200 * 200;
	private int urlLength = 500;
	
	private ThreadSafeClientConnManager _cm;
	
	private int numOfFetchers = 48;
	
	private BlockingQueue<WebPage> _queue;
	private BlockingQueue<List<Object>> _tupleQueue;
	
	public ArticleExtractionBolt(int numOfFetchers) {
		this.numOfFetchers = numOfFetchers;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("url", "expandedUrl", "type", "content"));
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		_collector = collector;
		_queue = new LinkedBlockingQueue<WebPage>();
		_tupleQueue =  new LinkedBlockingQueue<List<Object>>();
		
		HttpParams params = new BasicHttpParams();
		HttpConnectionParams.setConnectionTimeout(params, 2000);
		HttpConnectionParams.setSoTimeout(params, 2000);
		
		_cm = new ThreadSafeClientConnManager();
		_cm.setMaxTotal(100);
		
		_httpclient = new DefaultHttpClient(_cm, params);

		_articleExtractor = CommonExtractors.ARTICLE_EXTRACTOR;
	    _extractor = CommonExtractors.KEEP_EVERYTHING_EXTRACTOR;
	    _imageExtractor = ImageExtractor.INSTANCE;
	    _estimator = SimpleEstimator.INSTANCE;	
	    
	    Thread emitter = new Thread(new Emitter(_collector, _tupleQueue));
	    emitter.start();
	    
	    Thread[] fetchers = new Thread[numOfFetchers];
	    for(int i=0;i<numOfFetchers; i++) {
	    	fetchers[i] = new Thread(new Fetcher(_queue));
	    	fetchers[i].start();
	    }
	    
	    
	}

	public void execute(Tuple tuple) {
		WebPage webPage = (WebPage) tuple.getValueByField("webPage");
		try {
			if(webPage != null)
				_queue.put(webPage);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}   
	
	private static class Emitter implements Runnable {

		private OutputCollector collector;
		private BlockingQueue<List<Object>> tupleQueue;

		private static int emits = 0;
		private static long t = System.currentTimeMillis();
		
		public Emitter(OutputCollector _collector, BlockingQueue<List<Object>> _tupleQueue) {
			this.collector = _collector;
			this.tupleQueue = _tupleQueue;
		}
		
		@Override
		public void run() {
			while(true) {
				List<Object> tuple = tupleQueue.poll();
				if(tuple != null) {
					synchronized(collector) {
						emits++;
						if(emits%50==0) {
							t = System.currentTimeMillis() - t;
							System.out.println(emits + " articles proccessed in " + (t/1000) + " sec. "
									+ tupleQueue.size() + " in queue!");
							t = System.currentTimeMillis();
						}
						collector.emit(tuple);
					}
				}
				else {
					Utils.sleep(200);
				}
			}
		}
		
	}
	
	private class Fetcher implements Runnable {

		private BlockingQueue<WebPage> queue;
		
		public Fetcher(BlockingQueue<WebPage> _queue) {
			this.queue = _queue;
		}
		
		@Override
		public void run() {
			while(true) {
				
				WebPage webPage = null;
				try {
					webPage = queue.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
					continue;
				}
				
				if(webPage==null) {
					continue;
				}
				
				String url = webPage.getUrl();
				String expandedUrl = webPage.getExpandedUrl();
				
				HttpGet httpget = null;
				try {
					
					httpget = new HttpGet(expandedUrl);
					HttpResponse response = _httpclient.execute(httpget);
					
					HttpEntity entity = response.getEntity();
					ContentType contentType = ContentType.get(entity);
	
					if(!contentType.getMimeType().equals(ContentType.TEXT_HTML.getMimeType())) {
						System.out.println("Not supported mime type");
						System.out.println(contentType.getMimeType()+ " - "+ContentType.TEXT_HTML.getMimeType());
						_tupleQueue.add(tuple(url, expandedUrl, "exception", "Not supported mime type"));
						continue;
					}
					
					InputStream input = entity.getContent();
					byte[] content = IOUtils.toByteArray(input);
					
					System.out.println("Content length: " + content.length);
					System.out.println("==================================================");
					
					Article article = getArticle(webPage, content);
					if(article != null) 
						_tupleQueue.add(tuple(url, expandedUrl, "article", article));
					else 
						_tupleQueue.add(tuple(url, expandedUrl, "exception", "Article is null!"));
					
					
				} catch (Exception e) {
					
					_tupleQueue.add(tuple(url, expandedUrl, "exception", e.getMessage()));
				}
				finally {
					if(httpget != null)
						httpget.abort();
				}
				
			}
		}
	}
	
	private Article getArticle(WebPage webPage, byte[] content) {  
		
		String base = webPage.getExpandedUrl();
		
		int pageHash = (base.hashCode() & 0x7FFFFFFF);
	  	try { 
	  		InputSource articelIS = new InputSource(new ByteArrayInputStream(content));
		  	TextDocument document = null, imgDoc = null;

	  		document = new BoilerpipeSAXInput(articelIS).getTextDocument();
	  		imgDoc = document.clone();
	  		
	  		TextDocumentStatistics dsBefore = new TextDocumentStatistics(document, false);
	  		synchronized(_articleExtractor) {
	  			_articleExtractor.process(document);
	  		}
	  		synchronized(_extractor) {
	  			_extractor.process(imgDoc);
	  		}
	  		TextDocumentStatistics dsAfter = new TextDocumentStatistics(document, false);
	  		
	  		boolean isLowQuality = true;
	  		synchronized(_estimator) {
	  			isLowQuality = _estimator.isLowQuality(dsBefore, dsAfter);
	  		}
	  	
	  		String title = document.getTitle();
	  		String text = document.getText(true, false);
	  		
	  		Article article = new Article(title, text);
	  		article.setLowQuality(isLowQuality);
	  		
	  		//List<MediaItem> media = extractAricleImages(imgDoc, webPage, base, pageHash, content);
	  		
	  		List<MediaItem> media = extractAllImages(base, title, webPage, pageHash, content);
	  		
	  		for(MediaItem mItem : media) {
	  			article.addMediaItem(mItem);
	  		}
			return article;
			
	  	} catch(Exception ex) {
	  		return null;
	  	}
	}
	
	public List<MediaItem> extractAricleImages(TextDocument document, WebPage webPage, String base, int pageHash, 
			byte[] content) throws IOException, BoilerpipeProcessingException {
		
		List<MediaItem> images = new ArrayList<MediaItem>();
		
		String ref = webPage.getUrl();
		InputSource imageslIS = new InputSource(new ByteArrayInputStream(content));
  		
  		List<Image> detectedImages;
  		synchronized(_imageExtractor) {
  			detectedImages = _imageExtractor.process(document, imageslIS);
  		}
  		for(Image image  : detectedImages) {
  			
  			Integer w = -1, h = -1;
  			try {
  				String width = image.getWidth();
  				String height = image.getHeight();
  	
  				w = Integer.parseInt(width);
  				h = Integer.parseInt(height);
  			}
  			catch(Exception e) {
  				continue;
  			}
  			
  			// filter small images
  			if(image.getArea() < minArea || w < minDim  || h < minDim) 
				continue;

			String src = image.getSrc();
			URL url = null;
			try {
				url = new URL(new URL(base), src);
				
				if(url.toString().length()>urlLength)
					continue;
				
				if(src.endsWith(".gif") || url.getPath().endsWith(".gif"))
					continue;
				
			} catch (Exception e) {
				continue;
			}
			
			String alt = image.getAlt();
			if(alt == null) {
				alt = webPage.getTitle();
			}
			
			MediaItem mediaItem = new MediaItem(url);
			
			// Create image unique id
			int imageHash = (url.hashCode() & 0x7FFFFFFF);
			
			mediaItem.setId("Web::"+pageHash+"_"+imageHash);
			mediaItem.setStreamId("Web");
			mediaItem.setType("image");
			mediaItem.setThumbnail(url.toString());
			
			mediaItem.setPageUrl(base.toString());
			mediaItem.setRefUrl(ref);
			
			mediaItem.setShares(webPage.getShares());
			
			mediaItem.setTitle(alt.trim());
			if(w != -1 && h != -1) 
				mediaItem.setSize(w, h);
			
			images.add(mediaItem);
		}
  		
  		return images;
	}
	
	public List<MediaItem> extractAllImages(String baseUri, String title, WebPage webPage, int pageHash, byte[] content) throws IOException {
		List<MediaItem> images = new ArrayList<MediaItem>();
		
		String ref = webPage.getUrl();
		
		String html = IOUtils.toString(new ByteArrayInputStream(content));
		Document doc = Jsoup.parse(html, baseUri);
		
		Elements elements = doc.getElementsByTag("img");
		for(Element img  : elements) {
  			
			String src = img.attr("src");
			String alt = img.attr("alt");
			String width = img.attr("width");
			String height = img.attr("height");
			
  			Integer w = -1, h = -1;
  			try {
  				if(width==null || height==null || width.equals("") || height.equals(""))
  					continue;
  				
  				w = Integer.parseInt(width);
  				h = Integer.parseInt(height);
  				
  			// filter small images
  	  			if( (w*h) < minArea || w < minDim  || h < minDim) 
  					continue;
  			}
  			catch(Exception e) {
  				
  			}

			URL url = null;
			try {
				url = new URL(src);
				
				if(url.toString().length()>urlLength)
					continue;
				
				if(src.endsWith(".gif") || url.getPath().endsWith(".gif"))
					continue;
				
			} catch (Exception e) {
				continue;
			}
			
			if(alt == null) {
				alt = title;
			}
			
			MediaItem mediaItem = new MediaItem(url);
			
			// Create image unique id
			int imageHash = (url.hashCode() & 0x7FFFFFFF);
			
			mediaItem.setId("Web::"+pageHash+"_"+imageHash);
			mediaItem.setStreamId("Web");
			mediaItem.setType("image");
			mediaItem.setThumbnail(url.toString());
			
			mediaItem.setPageUrl(baseUri);
			mediaItem.setRefUrl(ref);
			
			mediaItem.setShares(webPage.getShares());
			
			mediaItem.setTitle(alt.trim());
			if(w != -1 && h != -1) 
				mediaItem.setSize(w, h);
			
			images.add(mediaItem);
		}
		return images;
	}
	
	/*
	public List<MediaItem> extractVideos(WebPage webPage, byte[] content) {
		
		String ref = webPage.getUrl();
		String base = webPage.getExpandedUrl();
		
		List<MediaItem> videos = new ArrayList<MediaItem>(); 
		int pageHash = (base.hashCode() & 0x7FFFFFFF);
		try {
			Document doc = Jsoup.parse(new ByteArrayInputStream(content), "UTF-8", base);
			
			Elements objects = doc.getElementsByTag("object");
			
			System.out.println(objects.size()+" objects");
			
			for(Element object :objects) {
				System.out.println(object);
				String data = object.attr("data");
				if(data == null || data.equals("")) {
					System.out.println("data is null");
					continue;
				}
				try {
					URL url = new URL(data);
					MediaItem mediaItem = new MediaItem(url);
					
					int imageHash = (url.hashCode() & 0x7FFFFFFF);
					mediaItem.setId("Web::"+pageHash+"_"+imageHash);
					mediaItem.setStreamId("Web");
					mediaItem.setType("video");
					mediaItem.setThumbnail(url.toString());
					
					mediaItem.setPageUrl(base.toString());
					mediaItem.setRefUrl(ref);
					
					mediaItem.setShares(webPage.getShares());
				}
				catch(Exception e) {
					e.printStackTrace();
					continue;
				}
			}
		} catch (Exception e) {
		}
		
		return videos;
	}
	*/
	
}