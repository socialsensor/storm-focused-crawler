package eu.socialsensor.focused.crawler.bolts.media;

import static backtype.storm.utils.Utils.tuple;

import java.util.Map;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.client.dao.impl.MediaItemDAOImpl;
import eu.socialsensor.framework.client.mongo.UpdateItem;
import eu.socialsensor.framework.common.domain.MediaItem;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class MediaUpdaterBolt extends BaseRichBolt {

    /**
	 * 
	 */
	private static final long serialVersionUID = -2548434425109192911L;
	
	Logger logger;
	
	private String mongodbHostname;
	private String mediaItemsDB;
	private String mediaItemsCollection;

	private MediaItemDAOImpl _mediaItemDAO;
	private OutputCollector _collector;

	public MediaUpdaterBolt(String mongodbHostname, String mediaItemsDB, String mediaItemsCollection) {
		this.mongodbHostname = mongodbHostname;
		this.mediaItemsDB = mediaItemsDB;
		this.mediaItemsCollection = mediaItemsCollection;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("MediaItem"));
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		
		logger = Logger.getLogger(MediaUpdaterBolt.class);
		try {
			_mediaItemDAO = new MediaItemDAOImpl(mongodbHostname, mediaItemsDB, mediaItemsCollection);
			_collector = collector;
		} catch (Exception e) {
			logger.error(e);
		}
		
	}

	public void execute(Tuple tuple) {
		if(_mediaItemDAO != null) {
			
			try {
			MediaItem mediaItem = (MediaItem) tuple.getValueByField("MediaItem");
				if(mediaItem == null)
					return;
			
				if(_mediaItemDAO.exists(mediaItem.getId())) {
				
					UpdateItem update = new UpdateItem();
					update.setField("vIndexed", mediaItem.isVisualIndexed());
					update.setField("status", mediaItem.isVisualIndexed()?"indexed":"failed");
				
					Integer width = mediaItem.getWidth();
					Integer height = mediaItem.getHeight();
					if(width!=null && height!=null && width!=-1 && height!=-1) {
						update.setField("height", height);
						update.setField("width", width);
					}
				
					_mediaItemDAO.updateMediaItem(mediaItem.getId(), update);
				}
				else {
					_collector.emit(tuple(mediaItem));
					_mediaItemDAO.addMediaItem(mediaItem);
				}
			}
			catch(Exception e) {
				logger.error(e);
			}
		}
	}   
	
}