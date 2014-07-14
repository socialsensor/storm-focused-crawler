package eu.socialsensor.focused.crawler.bolts.media;

import java.io.InputStream;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.MediaItem;
import static backtype.storm.utils.Utils.tuple;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class MediaFetcherBolt extends BaseRichBolt {

	private static final long serialVersionUID = -5514715036795163046L;
	
	private Logger _logger;
	
	private OutputCollector _collector;

	private CloseableHttpClient _httpclient;

	private RequestConfig _requestConfig;
	
	public MediaFetcherBolt() throws Exception {

	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		_logger = Logger.getLogger(MediaFetcherBolt.class);
		
		_collector = collector;

		_requestConfig = RequestConfig.custom()
		        .setSocketTimeout(30000)
		        .setConnectTimeout(30000)
		        .build();
		
		PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
		_httpclient = HttpClients.custom()
		        .setConnectionManager(cm)
		        .build();
	}

	public void execute(Tuple tuple) {
		MediaItem mediaItem = (MediaItem) tuple.getValueByField("MediaItem");
		if(mediaItem == null)
			return;
		
		HttpGet httpget = null;
		try {
			String id = mediaItem.getId();
			String type = mediaItem.getType();
			
			String url = type.equals("image") ? mediaItem.getUrl() : mediaItem.getThumbnail();
			
			httpget = new HttpGet(url.replaceAll(" ", "%20"));
			httpget.setConfig(_requestConfig);
			HttpResponse response = _httpclient.execute(httpget);
			
			StatusLine status = response.getStatusLine();
			int code = status.getStatusCode();
			
			if(code<200 || code>=300) {
				_logger.error("Failed fetch media item " + id + ". URL=" + url +  
						". Http code: " + code + " Error: " + status.getReasonPhrase());
				
				mediaItem.setVisualIndexed(false);
				_collector.emit(tuple(mediaItem, null));
				
				return;
			}
			
			HttpEntity entity = response.getEntity();
			if(entity == null) {
				_logger.error("Entity is null for " + id + ". URL=" + url +  
						". Http code: " + code + " Error: " + status.getReasonPhrase());
				
				mediaItem.setVisualIndexed(false);
				_collector.emit(tuple(mediaItem, null));
				
				return;
			}
			
			InputStream input = entity.getContent();
			byte[] imageContent = IOUtils.toByteArray(input);
			
			_collector.emit(tuple(mediaItem, imageContent));
			
		} 
		catch (Exception e) {
			_logger.error(e);
			_collector.emit(tuple(mediaItem, null));
		}
		finally {
			if(httpget != null) {
				httpget.abort();
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("MediaItem", "ImageContent"));
	}
	
}
