package eu.socialsensor.focused.crawler.bolts.webpages;

import static backtype.storm.utils.Utils.tuple;

import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.WebPage;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

public class WebPageFetcherBolt extends BaseRichBolt {

	private static final long serialVersionUID = -2548434425109192911L;
	
	private Logger _logger;
	
	private OutputCollector _collector;
	private HttpClient _httpclient;
	
	private PoolingHttpClientConnectionManager _cm;
	
	private int numOfFetchers = 24;
	
	private BlockingQueue<WebPage> _queue;
	private BlockingQueue<Pair<WebPage, byte[]>> _tupleQueue;

	private RequestConfig _requestConfig;

	private long receivedTuples = 0;
	
	private Thread _emitter;
	private List<Thread> _fetchers;
	
	public WebPageFetcherBolt() {

	}
	
	public WebPageFetcherBolt(int numOfFetchers) {
		this.numOfFetchers = numOfFetchers;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	//declarer.declareStream(MEDIA_STREAM, new Fields("MediaItem", "ImageContent"));
    	//declarer.declareStream(WEBPAGE_STREAM, new Fields("WebPage", "webPageContent"));
    	
    	declarer.declare(new Fields("WebPage", "Content"));
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		
		_logger = Logger.getLogger(WebPageFetcherBolt.class);
		
		_collector = collector;
		
		_queue = new LinkedBlockingQueue<WebPage>();
		_tupleQueue =  new LinkedBlockingQueue<Pair<WebPage, byte[]>>();
		
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
		private BlockingQueue<Pair<WebPage, byte[]>> _tupleQueue;
		
		private int mediaTuples = 0, webPagesTuples = 0;
		
		public Emitter(OutputCollector collector, BlockingQueue<Pair<WebPage, byte[]>> tupleQueue) {
			_collector = collector;
			_tupleQueue = tupleQueue;
		}
		
		public void run() {
			while(true) {
				Pair<?, ?> obj = _tupleQueue.poll();
				if(obj != null) {
					synchronized(_collector) {
						_collector.emit(tuple(obj.getLeft(), obj.getRight()));
						
						//if(MediaItem.class.isInstance(obj)) {
						//	mediaTuples++;
						//	_collector.emit(MEDIA_STREAM, tuple(obj));
						//}
						//else if(WebPage.class.isInstance(obj)) {
						//	webPagesTuples++;
						//	_collector.emit(WEBPAGE_STREAM, tuple(obj));
						//}
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
					_tupleQueue.add(Pair.of(webPage, new byte[0]));
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
						
						_tupleQueue.add(Pair.of(webPage, new byte[0]));
						
						continue;
					}
					
					InputStream input = entity.getContent();
					byte[] content = IOUtils.toByteArray(input);
					
					_tupleQueue.add(Pair.of(webPage, content));
					
				} catch (Exception e) {
					_logger.error("for " + expandedUrl, e);
					_tupleQueue.add(Pair.of(webPage, new byte[0]));
				}
				finally {
					if(httpget != null)
						httpget.abort();
				}
				
			}
		}
	}
	
}
