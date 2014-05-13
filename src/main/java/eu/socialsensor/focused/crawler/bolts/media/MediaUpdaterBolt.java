package eu.socialsensor.focused.crawler.bolts.media;

import static backtype.storm.utils.Utils.tuple;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.client.dao.MediaItemDAO;
import eu.socialsensor.framework.client.dao.StreamUserDAO;
import eu.socialsensor.framework.client.dao.impl.MediaItemDAOImpl;
import eu.socialsensor.framework.client.dao.impl.StreamUserDAOImpl;
import eu.socialsensor.framework.client.mongo.UpdateItem;
import eu.socialsensor.framework.common.domain.Concept;
import eu.socialsensor.framework.common.domain.MediaItem;
import eu.socialsensor.framework.common.domain.StreamUser;
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
	private String streamUsersDB;
	private String streamUsersCollection;
	
	private MediaItemDAO _mediaItemDAO;
	private StreamUserDAO _streamUsersDAO;
	private OutputCollector _collector;

	private long received = 0;
	private long newMedia=0, existedMedia = 0;
	
	public MediaUpdaterBolt(String mongodbHostname, String mediaItemsDB, String mediaItemsCollection, 
			String streamUsersDB, String streamUsersCollection) {
		
		this.mongodbHostname = mongodbHostname;
		this.mediaItemsDB = mediaItemsDB;
		this.mediaItemsCollection = mediaItemsCollection;
		
		this.streamUsersDB = streamUsersDB;
		this.streamUsersCollection = streamUsersCollection;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("MediaItem"));
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		
		logger = Logger.getLogger(MediaUpdaterBolt.class);
		try {
			_mediaItemDAO = new MediaItemDAOImpl(mongodbHostname, mediaItemsDB, mediaItemsCollection);
			_streamUsersDAO = new StreamUserDAOImpl(mongodbHostname, streamUsersDB, streamUsersCollection);
			_collector = collector;
		} catch (Exception e) {
			logger.error(e);
		}
		
	}

	public void execute(Tuple tuple) {
		if(_mediaItemDAO != null) {
			
			
			try {
				
			if(++received%1000==0) {
				logger.info(received + " media items received. " + newMedia + " are new and "
						+ existedMedia + " already exists!");
			}
				
			MediaItem mediaItem = (MediaItem) tuple.getValueByField("MediaItem");
				if(mediaItem == null)
					return;
			
				if(_mediaItemDAO.exists(mediaItem.getId())) {
				
					existedMedia++;
					
					UpdateItem update = new UpdateItem();
					update.setField("vIndexed", mediaItem.isVisualIndexed());
					update.setField("status", mediaItem.isVisualIndexed()?"indexed":"failed");
				
					Integer width = mediaItem.getWidth();
					Integer height = mediaItem.getHeight();
					if(width!=null && height!=null && width!=-1 && height!=-1) {
						update.setField("height", height);
						update.setField("width", width);
					}
				
					List<Concept> concepts = mediaItem.getConcepts();
					if(concepts != null) {
						update.setField("concepts", concepts);
					}
					
					_mediaItemDAO.updateMediaItem(mediaItem.getId(), update);
				}
				else {
					newMedia++;
					
					// Emit for indexing 
					_collector.emit(tuple(mediaItem));
					
					_mediaItemDAO.addMediaItem(mediaItem);
					
					StreamUser user = mediaItem.getUser();
					if(user != null) {
						if(!_streamUsersDAO.exists(user.getId())) {
							_streamUsersDAO.insertStreamUser(user);
						}
					}
				}
			}
			catch(Exception e) {
				logger.error(e);
			}
		}
	}   
	
}