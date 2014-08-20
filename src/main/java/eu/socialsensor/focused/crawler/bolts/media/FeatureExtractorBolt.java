package eu.socialsensor.focused.crawler.bolts.media;

import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

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

public class FeatureExtractorBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5514715036795163046L;
	
	private Logger _logger;
	
	private OutputCollector _collector;

	private CloseableHttpClient _httpclient;

	private RequestConfig _requestConfig;

	private static int[] numCentroids = { 128, 128, 128, 128 };
	private static int targetLengthMax = 1024;
	
	private static int maxNumPixels = 768 * 512; // use 1024*768 for better/slower extraction
	
	public FeatureExtractorBolt(String webServiceHost, String indexCollection, String[] codebookFiles, String pcaFile) throws Exception {

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
		
		_logger = Logger.getLogger(FeatureExtractorBolt.class);
		
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
		ImageVector imageVector = null;
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
				_collector.emit(tuple(mediaItem, imageVector));
				
				return;
			}
			
			HttpEntity entity = response.getEntity();
			if(entity == null) {
				_logger.error("Entity is null for " + id + ". URL=" + url +  
						". Http code: " + code + " Error: " + status.getReasonPhrase());
				
				mediaItem.setVisualIndexed(false);
				_collector.emit(tuple(mediaItem, imageVector));
				
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
			}
			_collector.emit(tuple(mediaItem, imageVector));
			
		} 
		catch (Exception e) {
			_logger.error(e);
			_collector.emit(tuple(mediaItem, imageVector));
		}
		finally {
			if(httpget != null)
				httpget.abort();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("MediaItem", "ImageVector"));
	}
	
	public static BufferedImage resize(BufferedImage img, int maxWidth, int maxHeight) { 
		
		int scaledWidth = 0, scaledHeight = 0;
		
		scaledWidth = maxWidth;
		scaledHeight = (int) (img.getHeight() * ( (double) scaledWidth / img.getWidth() ));
		if (scaledHeight> maxHeight) {
	        scaledHeight = maxHeight;
	        scaledWidth= (int) (img.getWidth() * ( (double) scaledHeight/ img.getHeight() ));

	        if (scaledWidth > maxWidth) {
	            scaledWidth = maxWidth;
	            scaledHeight = maxHeight;
	        }
	    }
		
		Image resized =  img.getScaledInstance( scaledWidth, scaledHeight, Image.SCALE_SMOOTH);
		BufferedImage buffered = new BufferedImage(scaledWidth, scaledHeight, Image.SCALE_REPLICATE);
		buffered.getGraphics().drawImage(resized, 0, 0 , null);
		
		//String formatName = getFormatName( ImageIO.createImageInputStream(buffered) ) ;
	    //ByteArrayOutputStream out = new ByteArrayOutputStream();
	    //ImageIO.write(buffered, formatName, out);
	    
		return buffered;
		
	    //int w = img.getWidth();  
	    //int h = img.getHeight();  
	    //BufferedImage dimg = new BufferedImage(newW, newH, img.getType());  
	    //Graphics2D g = dimg.createGraphics();  
	    //g.setRenderingHint(RenderingHints.KEY_INTERPOLATION,
	    //RenderingHints.VALUE_INTERPOLATION_BILINEAR);  
	    //g.drawImage(img, 0, 0, scaledWidth, scaledHeight, 0, 0, w, h, null);  
	    //g.dispose();  
	    //return dimg;  
	}  
	
	public static String getFormatName(ImageInputStream iis) {
	    try { 

	        // Find all image readers that recognize the image format
	        @SuppressWarnings("rawtypes")
			Iterator iter = ImageIO.getImageReaders(iis);
	        if (!iter.hasNext()) {
	            // No readers found
	            return null;
	        }

	        // Use the first reader
	        ImageReader reader = (ImageReader)iter.next();

	        // Close stream
	        iis.close();

	        // Return the format name
	        return reader.getFormatName();
	    } catch (IOException e) {
	    }

	    return null;
	}
}
