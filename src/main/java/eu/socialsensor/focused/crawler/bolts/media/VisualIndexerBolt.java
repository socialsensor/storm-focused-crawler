package eu.socialsensor.focused.crawler.bolts.media;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

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

import eu.socialsensor.focused.crawler.models.ImageVector;
import eu.socialsensor.framework.client.search.visual.JsonResultSet;
import eu.socialsensor.framework.client.search.visual.JsonResultSet.JsonResult;
import eu.socialsensor.framework.client.search.visual.VisualIndexHandler;
import eu.socialsensor.framework.common.domain.MediaItem;
import gr.iti.mklab.visual.aggregation.VladAggregatorMultipleVocabularies;
import gr.iti.mklab.visual.dimreduction.PCA;
import gr.iti.mklab.visual.extraction.AbstractFeatureExtractor;
import gr.iti.mklab.visual.extraction.SURFExtractor;
import gr.iti.mklab.visual.vectorization.ImageVectorization;
import gr.iti.mklab.visual.vectorization.ImageVectorizationResult;
import static backtype.storm.utils.Utils.tuple;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class VisualIndexerBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5514715036795163046L;
	
	private Logger _logger;
	
	private OutputCollector _collector;
	private VisualIndexHandler _visualIndex;

	private String _webServiceHost;
	private String _indexCollection;

	private CloseableHttpClient _httpclient;

	private RequestConfig _requestConfig;

	private static int[] numCentroids = { 128, 128, 128, 128 };
	private static int targetLengthMax = 1024;
	
	private static int maxNumPixels = 768 * 512; // use 1024*768 for better/slower extraction
	
	public VisualIndexerBolt(String webServiceHost, String indexCollection, String[] codebookFiles, String pcaFile) throws Exception {
		
		_webServiceHost = webServiceHost;
		_indexCollection = indexCollection;
		
		ImageVectorization.setFeatureExtractor(new SURFExtractor());
		
		VladAggregatorMultipleVocabularies vladAggregator = new VladAggregatorMultipleVocabularies(codebookFiles, numCentroids, 
				AbstractFeatureExtractor.SURFLength);
		
		ImageVectorization.setVladAggregator(vladAggregator);
		
		int initialLength = numCentroids.length * numCentroids[0] * AbstractFeatureExtractor.SURFLength;
		if(initialLength > targetLengthMax) {
			PCA pca = new PCA(targetLengthMax, 1, initialLength, true);
			pca.loadPCAFromFile(pcaFile);
			ImageVectorization.setPcaProjector(pca);
		}
		
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		_logger = Logger.getLogger(VisualIndexerBolt.class);
		
		_collector = collector;
		_visualIndex = new VisualIndexHandler(_webServiceHost, _indexCollection);

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
		
		ImageVector imageVector = null;
		String nearestMediaItem = null;
		
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
				_collector.emit(tuple(mediaItem, imageVector, nearestMediaItem));
				
				return;
			}
			
			HttpEntity entity = response.getEntity();
			if(entity == null) {
				_logger.error("Entity is null for " + id + ". URL=" + url +  
						". Http code: " + code + " Error: " + status.getReasonPhrase());
				
				mediaItem.setVisualIndexed(false);
				_collector.emit(tuple(mediaItem, imageVector, nearestMediaItem));
				
				return;
			}
			
			InputStream input = entity.getContent();
			byte[] imageContent = IOUtils.toByteArray(input);
			
			BufferedImage image = ImageIO.read(new ByteArrayInputStream(imageContent));
		
			if(image != null) {
				ImageVectorization imvec = new ImageVectorization(id, image, targetLengthMax, maxNumPixels);
				
				if(mediaItem.getWidth()==null && mediaItem.getHeight()==null) {
					mediaItem.setSize(image.getWidth(), image.getHeight());
				}

				ImageVectorizationResult imvr = imvec.call();
				double[] vector = imvr.getImageVector();
				
				if(vector==null || vector.length==0) {
					_logger.error("Error in feature extraction for " + id);
				}
			
				imageVector = new ImageVector(id, url, vector);
				//boolean indexed = _visualIndex.index(id, vector);
				
				JsonResultSet similar = _visualIndex.getSimilarImagesAndIndex(id, vector, 0.85);
				List<JsonResult> nearestMediaItems = similar.getResults();
				if(!nearestMediaItems.isEmpty()) {
					nearestMediaItem = nearestMediaItems.get(0).getId();
				}
				
				
			}
			
			mediaItem.setVisualIndexed(true);
			_collector.emit(tuple(mediaItem, imageVector, nearestMediaItem));
			
		} 
		catch (Exception e) {
			_logger.error(e);
			mediaItem.setVisualIndexed(false);
			_collector.emit(tuple(mediaItem, imageVector, nearestMediaItem));
		}
		finally {
			if(httpget != null) {
				httpget.abort();
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("MediaItem", "ImageVector", "nearestMediaItem"));
	}
	
}
