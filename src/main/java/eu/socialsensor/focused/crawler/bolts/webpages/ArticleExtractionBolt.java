package eu.socialsensor.focused.crawler.bolts.webpages;

import static backtype.storm.utils.Utils.tuple;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.xml.sax.InputSource;

import de.l3s.boilerpipe.BoilerpipeExtractor;
import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.document.TextDocument;
import de.l3s.boilerpipe.document.TextDocumentStatistics;
import de.l3s.boilerpipe.estimators.SimpleEstimator;
import de.l3s.boilerpipe.extractors.CommonExtractors;
import de.l3s.boilerpipe.sax.BoilerpipeSAXInput;
import eu.socialsensor.focused.crawler.models.Article;
import eu.socialsensor.focused.crawler.utils.Image;
import eu.socialsensor.focused.crawler.utils.ImageExtractor;
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

	private static final long serialVersionUID = -2548434425109192911L;
	
	private static final String SUCCESS = "success";
	private static final String FAILED = "failed";
	
	public static String MEDIA_STREAM = "media";
	public static String WEBPAGE_STREAM = "webpage";
	
	private Logger _logger;
	
	private OutputCollector _collector;
	private HttpClient _httpclient;
	
	private BoilerpipeExtractor _extractor, _articleExtractor;
	private ImageExtractor _imageExtractor;
	private SimpleEstimator _estimator;
	
	private int minDim = 200;
	private int minArea = 200 * 200;
	private int maxUrlLength = 500;
	
	private PoolingHttpClientConnectionManager _cm;
	
	private int numOfFetchers = 24;
	
	private BlockingQueue<WebPage> _queue;
	private BlockingQueue<Object> _tupleQueue;

	private RequestConfig _requestConfig;

	private long receivedTuples = 0;
	
	private Thread _emitter;
	private List<Thread> _fetchers;
	
	public ArticleExtractionBolt() {

	}
	
	public ArticleExtractionBolt(int numOfFetchers) {
		this.numOfFetchers = numOfFetchers;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declareStream(MEDIA_STREAM, new Fields("MediaItem"));
    	declarer.declareStream(WEBPAGE_STREAM, new Fields("WebPage"));
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		
		_logger = Logger.getLogger(ArticleExtractionBolt.class);
		
		_collector = collector;
		
		_queue = new LinkedBlockingQueue<WebPage>();
		_tupleQueue =  new LinkedBlockingQueue<Object>();
		
		_cm = new PoolingHttpClientConnectionManager();
		_cm.setMaxTotal(numOfFetchers);
		_cm.setDefaultMaxPerRoute(10);

		_httpclient = HttpClients.custom()
		        .setConnectionManager(_cm)
		        .build();
		
		// Set timeout parameters for Http requests
		_requestConfig = RequestConfig.custom()
		        .setSocketTimeout(30000)
		        .setConnectTimeout(30000)
		        .build();

		_articleExtractor = CommonExtractors.ARTICLE_EXTRACTOR;
	    _extractor = CommonExtractors.ARTICLE_EXTRACTOR;
	    // The use of Canola extractor increases recall of the returned media items but decreases precision.
	    //_extractor = CommonExtractors.CANOLA_EXTRACTOR;
	    
	    _imageExtractor = ImageExtractor.INSTANCE;
	    
	    // Quality estimator of the article extraction process
	    _estimator = SimpleEstimator.INSTANCE;	
	    
	    _emitter = new Thread(new Emitter(_collector, _tupleQueue));
	    _emitter.start();
	    
	    _fetchers = new ArrayList<Thread>(numOfFetchers);
	    for(int i=0;i<numOfFetchers; i++) {
	    	Thread fetcher = new Thread(new HttpFetcher(_queue));
	    	fetcher.start();
	    	
	    	_fetchers.add(fetcher);
	    }
	}

	public void execute(Tuple tuple) {
		receivedTuples++;
		WebPage webPage = (WebPage) tuple.getValueByField("webPage");
		try {
			if(webPage != null) {
				_queue.put(webPage);
			}
		} catch (InterruptedException e) {
			_logger.error(e);
		}
	}   
	
	private class Emitter implements Runnable {

		private OutputCollector _collector;
		private BlockingQueue<Object> _tupleQueue;
		
		private int mediaTuples = 0, webPagesTuples = 0;
		
		public Emitter(OutputCollector collector, BlockingQueue<Object> tupleQueue) {
			_collector = collector;
			_tupleQueue = tupleQueue;
		}
		
		public void run() {
			while(true) {
				Object obj = _tupleQueue.poll();
				if(obj != null) {
					synchronized(_collector) {
						if(MediaItem.class.isInstance(obj)) {
							mediaTuples++;
							_collector.emit(MEDIA_STREAM, tuple(obj));
						}
						else if(WebPage.class.isInstance(obj)) {
							webPagesTuples++;
							_collector.emit(WEBPAGE_STREAM, tuple(obj));
						}
					}
				}
				else {
					Utils.sleep(500);
				}
				
				if((mediaTuples%100==0 || webPagesTuples%100==0) && (mediaTuples!=0 || webPagesTuples!=0)) {
					_logger.info(receivedTuples + " tuples received, " + mediaTuples + " media tuples emmited, " + 
							webPagesTuples + " web page tuples emmited");
					_logger.info(getWorkingFetchers() + " fetchers out of " + numOfFetchers + " are working.");
				}
			}
		}
	}
	
	private int getWorkingFetchers() {
		int working = 0;
		for(Thread fetcher : _fetchers) {
			if(fetcher.isAlive())
				working++;
		}
		return working;
	}
	
	private class HttpFetcher implements Runnable {

		private BlockingQueue<WebPage> queue;
		
		public HttpFetcher(BlockingQueue<WebPage> _queue) {
			this.queue = _queue;
		}
		
		public void run() {
			while(true) {
				
				WebPage webPage = null;
				try {
					webPage = queue.take();
				} catch (Exception e) {
					_logger.error(e);
					continue;
				}
				
				if(webPage == null) {
					continue;
				}
				
				String expandedUrl = webPage.getExpandedUrl();
				if(expandedUrl==null || expandedUrl.length()>300) {
					webPage.setStatus(FAILED);
					_tupleQueue.add(webPage);
					
					continue;
				}
				
				HttpGet httpget = null;
				try {
					
					URI uri = new URI(expandedUrl
							.replaceAll(" ", "%20")
							.replaceAll("\\|", "%7C")
							);
					
					httpget = new HttpGet(uri);
					httpget.setConfig(_requestConfig);
					HttpResponse response = _httpclient.execute(httpget);
					
					HttpEntity entity = response.getEntity();
					ContentType contentType = ContentType.get(entity);
	
					if(!contentType.getMimeType().equals(ContentType.TEXT_HTML.getMimeType())) {
						_logger.error("URL: " + webPage.getExpandedUrl() + 
								"   Not supported mime type: " + contentType.getMimeType());
						
						webPage.setStatus(FAILED);
						_tupleQueue.add(webPage);
						
						continue;
					}
					
					InputStream input = entity.getContent();
					byte[] content = IOUtils.toByteArray(input);
					
					List<MediaItem> mediaItems = new ArrayList<MediaItem>();
					boolean parsed = parseWebPage(webPage, content, mediaItems);
					if(parsed) { 
						webPage.setStatus(SUCCESS);
						_tupleQueue.add(webPage);
						for(MediaItem mItem : mediaItems) {
							_tupleQueue.add(mItem);
						}
					}
					else {
						_logger.error("Parsing of " + expandedUrl + " failed.");
						webPage.setStatus(FAILED);
						_tupleQueue.add(webPage);
					}
				} catch (Exception e) {
					_logger.error("for " + expandedUrl, e);
					webPage.setStatus(FAILED);
					_tupleQueue.add(webPage);
				}
				finally {
					if(httpget != null)
						httpget.abort();
				}
				
			}
		}
	}
	
	public boolean parseWebPage(WebPage webPage, byte[] content, List<MediaItem> mediaItems) {  
	  	try { 
	  		String base = webPage.getExpandedUrl();
	  		
	  		InputSource articelIS1 = new InputSource(new ByteArrayInputStream(content));
	  		InputSource articelIS2 = new InputSource(new ByteArrayInputStream(content));
	  		
		  	TextDocument document = null, imgDoc = null;

	  		document = new BoilerpipeSAXInput(articelIS1).getTextDocument();
	  		imgDoc = new BoilerpipeSAXInput(articelIS2).getTextDocument();
	  		
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

	  		webPage.setTitle(title);
	  		webPage.setText(text);
	  		webPage.setArticle(!isLowQuality);
	  		
	  		mediaItems.addAll(extractArticleImages(imgDoc, webPage, base, content));		
	  		webPage.setMedia(mediaItems.size());
	  		
	  		List<String> mediaIds = new ArrayList<String>();
	  		for(MediaItem mediaItem : mediaItems) {
	  			mediaIds.add(mediaItem.getId());
	  		}
	  		webPage.setMediaIds(mediaIds.toArray(new String[mediaIds.size()]));
	  		
	  		if(mediaItems.size() > 0) {
	  			MediaItem mediaItem = mediaItems.get(0);
	  			webPage.setMediaThumbnail(mediaItem.getUrl());
	  		}
	  		
			return true;
			
	  	} catch(Exception ex) {
	  		_logger.error(ex);
	  		return false;
	  	}
	}

	public Article getArticle(WebPage webPage, byte[] content) {  
		
		String base = webPage.getExpandedUrl();
		
	  	try { 
	  		InputSource articelIS1 = new InputSource(new ByteArrayInputStream(content));
	  		InputSource articelIS2 = new InputSource(new ByteArrayInputStream(content));
		  	TextDocument document = null, imgDoc = null;

	  		document = new BoilerpipeSAXInput(articelIS1).getTextDocument();
	  		imgDoc = new BoilerpipeSAXInput(articelIS2).getTextDocument();
	  		
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
	  		
	  		List<MediaItem> mediaItems = extractArticleImages(imgDoc, webPage, base, content);		
	  		//List<MediaItem> mediaItems = extractAllImages(base, title, webPage, pageHash, content);
	  		
	  		for(MediaItem mItem : mediaItems) {
	  			article.addMediaItem(mItem);
	  		}
			return article;
			
	  	} catch(Exception ex) {
	  		_logger.error(ex);
	  		return null;
	  	}
	}
	
	public List<MediaItem> extractArticleImages(TextDocument document, WebPage webPage, String base, byte[] content) 
			throws IOException, BoilerpipeProcessingException {
		
		List<MediaItem> mediaItems = new ArrayList<MediaItem>();
		
		InputSource imageslIS = new InputSource(new ByteArrayInputStream(content));
  		
  		List<Image> detectedImages;
  		synchronized(_imageExtractor) {
  			detectedImages = _imageExtractor.process(document, imageslIS);
  		}
  		
  		for(Image image  : detectedImages) {
  			Integer w = -1, h = -1;
  			try {
  				String width = image.getWidth().replaceAll("%", "");
  				String height = image.getHeight().replaceAll("%", "");
  	
  				w = Integer.parseInt(width);
  				h = Integer.parseInt(height);
  			}
  			catch(Exception e) {
  				// filter images without size
  				continue;
  			}
  			
  			// filter small images
  			if(image.getArea() < minArea || w < minDim  || h < minDim) 
				continue;

			String src = image.getSrc();
			URL url = null;
			try {
				url = new URL(new URL(base), src);
				
				if(url.toString().length() > maxUrlLength)
					continue;
				
				if(src.endsWith(".gif") || url.getPath().endsWith(".gif"))
					continue;
				
			} catch (Exception e) {
				_logger.error("Error for " + src + " in " + base, e);
				continue;
			}
			
			String alt = image.getAlt();
			if(alt == null) {
				alt = webPage.getTitle();
				if(alt == null)
					continue;
			}
			
			MediaItem mediaItem = new MediaItem(url);
			
			// Create image unique id. Is this a good practice? 
			int imageHash = (url.hashCode() & 0x7FFFFFFF);
			
			mediaItem.setId("Web#" + imageHash);
			mediaItem.setStreamId("Web");
			mediaItem.setType("image");
			mediaItem.setThumbnail(url.toString());
			
			mediaItem.setPageUrl(base.toString());
			mediaItem.setRef(webPage.getReference());
			
			mediaItem.setShares((long)webPage.getShares());
			
			mediaItem.setTitle(alt.trim());
			mediaItem.setDescription(webPage.getTitle());
			
			if(w != -1 && h != -1) 
				mediaItem.setSize(w, h);
			
			if(webPage.getDate() != null)
				mediaItem.setPublicationTime(webPage.getDate().getTime());
			
			mediaItems.add(mediaItem);
		}
  		return mediaItems;
	}
	
	
	public List<MediaItem> extractAllImages(String baseUri, String title, WebPage webPage, byte[] content) throws IOException {
		List<MediaItem> images = new ArrayList<MediaItem>();
		
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
  				_logger.error(e);
  			}

			URL url = null;
			try {
				url = new URL(src);
				
				if(url.toString().length() > maxUrlLength)
					continue;
				
				if(src.endsWith(".gif") || url.getPath().endsWith(".gif"))
					continue;
				
			} catch (Exception e) {
				_logger.error(e);
				continue;
			}
			
			if(alt == null) {
				alt = title;
			}
			
			MediaItem mediaItem = new MediaItem(url);
			
			// Create image unique id
			String urlStr = url.toString().trim();
			int imageHash = (urlStr.hashCode() & 0x7FFFFFFF);
			
			mediaItem.setId("Web#" + imageHash);
			mediaItem.setStreamId("Web");
			mediaItem.setType("image");
			mediaItem.setThumbnail(url.toString());
			
			mediaItem.setPageUrl(baseUri);
			
			mediaItem.setShares((long)webPage.getShares());
			mediaItem.setTitle(alt.trim());
			mediaItem.setDescription(webPage.getTitle());
			
			if(w != -1 && h != -1) 
				mediaItem.setSize(w, h);
			
			if(webPage.getDate() != null)
				mediaItem.setPublicationTime(webPage.getDate().getTime());
			
			images.add(mediaItem);
		}
		return images;
	}
	
	public List<MediaItem> extractVideos(WebPage webPage, byte[] content) {

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
					mediaItem.setId("Web#"+pageHash+"_"+imageHash);
					mediaItem.setStreamId("Web");
					mediaItem.setType("video");
					mediaItem.setThumbnail(url.toString());
					
					mediaItem.setPageUrl(base.toString());
					
					mediaItem.setShares((long) webPage.getShares());
				}
				catch(Exception e) {
					e.printStackTrace();
					continue;
				}
			}
		} catch (Exception e) {
			_logger.error(e);
		}
		
		return videos;
	}
}
