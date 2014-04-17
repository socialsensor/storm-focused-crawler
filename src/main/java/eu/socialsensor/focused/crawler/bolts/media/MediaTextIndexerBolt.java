package eu.socialsensor.focused.crawler.bolts.media;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import eu.socialsensor.focused.crawler.models.Article;
import eu.socialsensor.framework.client.search.solr.SolrMediaItemHandler;
import eu.socialsensor.framework.common.domain.MediaItem;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;


public class MediaTextIndexerBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7500656732029697927L;
	
	private Logger logger;
	
	private String hostname;
	private String service;
	private String collection;

	private SolrMediaItemHandler solrMediaHandler;
	
	public MediaTextIndexerBolt(String hostname, String service, String collection) {
		this.hostname = hostname;
		this.service = service;
		this.collection = collection;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		logger = Logger.getLogger(MediaTextIndexerBolt.class);
		
		try {
			solrMediaHandler = SolrMediaItemHandler.getInstance(hostname+"/"+service+"/"+collection);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e);
			solrMediaHandler = null;
		}
	}

	public void execute(Tuple tuple) {
		
		try {
			String url = tuple.getStringByField("url");
			String type = tuple.getStringByField("type");
		
			if(url == null || type == null || solrMediaHandler==null)
				return;
		
			if(type.equals("media")) {
				MediaItem mediaItem = (MediaItem) tuple.getValueByField("content");
				solrMediaHandler.insertMediaItem(mediaItem);
			}
			else if(type.equals("article")) {
				Article article = (Article) tuple.getValueByField("content");

				List<MediaItem> mediaItems = article.getMediaItems();	
				solrMediaHandler.insertMediaItems(mediaItems);
				
			}
			else {
				//Nothing todo
				logger.error("Unsupported type!");
			}
		}
		catch(Exception ex) {
			ex.printStackTrace();
			logger.error(ex);
		}
		
	}
 
}