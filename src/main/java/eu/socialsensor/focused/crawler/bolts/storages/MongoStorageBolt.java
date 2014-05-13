package eu.socialsensor.focused.crawler.bolts.storages;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class MongoStorageBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2613697672344106360L;

	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	/**
	 * 
	 *
	private static final long serialVersionUID = -2613697672344106360L;

	private static String HOST = "mongodb.host";

	private static String ITEMS_DATABASE = "mongodb.items.database";
	private static String ITEMS_COLLECTION = "mongodb.items.collection";
	
	private static String MEDIA_ITEMS_DATABASE = "mongodb.mediaitems.database";
	private static String MEDIA_ITEMS_COLLECTION = "mongodb.mediaitems.collection";
	
	private static String MEDIA_SHARES_DATABASE = "mongodb.mediashares.database";
	private static String MEDIA_SHARES_COLLECTION = "mongodb.mediashares.collection";
	
	private static String USERS_DATABASE = "mongodb.streamusers.database";
	private static String USERS_COLLECTION = "mongodb.streamusers.collection";
	
	private static String WEBPAGES_DATABASE = "mongodb.webpages.database";
	private static String WEBPAGES_COLLECTION = "mongodb.webpages.collection";

	private String _host;
	private String _itemsDbName, _itemsCollectionName;
	private String _mediaItemsDbName, _mediaItemsCollectionName;
	private String _mediaSharesDbName, _mediaSharesCollectionName;
	private String _streamUsersDbName, _streamUsersCollectionName;
	private String _webPageDbName, _webPageCollectionName;

	private Logger _logger;

	// DAOs
	private ItemDAOImpl _itemDAO = null;
	private MediaItemDAOImpl _mediaItemDAO = null;
	private MediaSharesDAOImpl _mediaSharesDAO = null;
	private StreamUserDAOImpl _streamUserDAO = null;
	private WebPageDAOImpl _webPageDAO = null;
	
	public MongoStorageBolt(StorageConfiguration config) {	
		this._host = config.getParameter(MongoStorageBolt.HOST);
		
		this._itemsDbName = config.getParameter(MongoStorageBolt.ITEMS_DATABASE);
		this._itemsCollectionName = config.getParameter(MongoStorageBolt.ITEMS_COLLECTION);
		
		this._mediaItemsDbName = config.getParameter(MongoStorageBolt.MEDIA_ITEMS_DATABASE);
		this._mediaItemsCollectionName = config.getParameter(MongoStorageBolt.MEDIA_ITEMS_COLLECTION);
		
		this._mediaSharesDbName = config.getParameter(MongoStorageBolt.MEDIA_SHARES_DATABASE);
		this._mediaSharesCollectionName = config.getParameter(MongoStorageBolt.MEDIA_SHARES_COLLECTION);
		
		this._streamUsersDbName = config.getParameter(MongoStorageBolt.USERS_DATABASE);
		this._streamUsersCollectionName = config.getParameter(MongoStorageBolt.USERS_COLLECTION);
		
		this._webPageDbName = config.getParameter(MongoStorageBolt.WEBPAGES_DATABASE);
		this._webPageCollectionName = config.getParameter(MongoStorageBolt.WEBPAGES_COLLECTION);
	
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_logger = Logger.getLogger(MongoDbStorage.class);
		try {
			
			if(_itemsCollectionName != null && _itemsDbName != null)
				this._itemDAO = new ItemDAOImpl(_host, _itemsDbName, _itemsCollectionName);
			
			if(_mediaItemsCollectionName != null && _mediaItemsDbName != null)
				this._mediaItemDAO = new MediaItemDAOImpl(_host, _mediaItemsDbName, _mediaItemsCollectionName);
			
			if(_mediaSharesCollectionName != null && _mediaSharesDbName != null)
				this._mediaSharesDAO = new MediaSharesDAOImpl(_host, _mediaSharesDbName, _mediaSharesCollectionName);
			
			if(_streamUsersCollectionName != null && _streamUsersDbName != null)
				this._streamUserDAO = new StreamUserDAOImpl(_host, _streamUsersDbName, _streamUsersCollectionName);
			
			if(_webPageCollectionName != null && _webPageDbName != null)
				this._webPageDAO = new WebPageDAOImpl(_host, _webPageDbName, _webPageCollectionName);
		
		} catch (Exception e) {
			_logger.error("MongoDB Storage bolt failed to be prepared");
			_logger.error(e);
		}
	}

	public void execute(Tuple input) {
		try {
			Item item = (Item) input.getValueByField("Item");
			if(item == null)
				return;
			
			store(item);
		}
		catch(Exception e) {
			_logger.error(e);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) { 
		
	}

	private void store(Item item) {
		try {
			// Handle Items
			String itemId = item.getId();
			if(!_itemDAO.exists(itemId)) {
				
				// Item does not exist in MongoDB. Save it.
				
				item.setInsertionTime(System.currentTimeMillis());
				_itemDAO.insertItem(item);
				
				// Handle Stream Users
				StreamUser user = item.getStreamUser();
				if(user != null) {
					
					String userId = user.getId();
					if(!_streamUserDAO.exists(userId)) {
						// save stream user
						_streamUserDAO.insertStreamUser(user);
					}
					else {
						// Update statistics of stream user
						
						synchronized(usersMap) {
							StreamUser tempUser = usersMap.get(user.getId());
							if(tempUser == null) {
								tempUser = new StreamUser(null, Operation.UPDATE);
								tempUser.setId(user.getId());
								usersMap.put(user.getId(), tempUser);
							}
							tempUser.incItems(1);
							tempUser.incMentions(1L);
						}
						
						
					}
				}
				
				if(item.getMentions() != null) {
					String[] mentionedUsers = item.getMentions();
					for(String mentionedUser : mentionedUsers) {
					
						synchronized(usersMap) {
							StreamUser tempUser = usersMap.get(mentionedUser);
							if(tempUser == null) {
								tempUser = new StreamUser(null, Operation.UPDATE);
								tempUser.setId(mentionedUser);
								usersMap.put(mentionedUser, tempUser);
							}
							tempUser.incMentions(1L);
						}
						
					}
				}

				if(item.getReferencedUserId() != null) {
					String userid = item.getReferencedUserId();
					synchronized(usersMap) {
						StreamUser tempUser = usersMap.get(userid);
						if(tempUser == null) {
							tempUser = new StreamUser(null, Operation.UPDATE);
							tempUser.setId(userid);
							usersMap.put(userid, tempUser);
						}
						tempUser.incShares(1L);
					}
				}
				
				// Handle Media Items
				for(MediaItem mediaItem : item.getMediaItems()) {
					if(!_mediaItemDAO.exists(mediaItem.getId())) {
						// MediaItem does not exist. Save it.
						_mediaItemDAO.addMediaItem(mediaItem);
					}
					else {
						//Update media item
					}
					
					if(_mediaSharesDAO != null) {
						_mediaSharesDAO.addMediaShare(mediaItem.getId(), mediaItem.getRef(), 
								mediaItem.getPublicationTime(), mediaItem.getUserId());
					}
				}
				
				// Handle Web Pages
				List<WebPage> webPages = item.getWebPages();
				if(webPages != null) {
					for(WebPage webPage : webPages) {
						String webPageURL = webPage.getUrl();
						if(!_webPageDAO.exists(webPageURL)) {
							// Web page does not exist. Save it.
							_webPageDAO.addWebPage(webPage);
						}
						else {
							synchronized(webpagesSharesMap) {
								Integer shares = webpagesSharesMap.get(webPageURL);
								if(shares == null)
									shares = 0;
								webpagesSharesMap.put(webPageURL, ++shares);
							}
						}
					}
				}
			}
			else {
				// Update 
			}
		}
		catch(MongoException e) {
			e.printStackTrace();
			System.out.println("Storing item " + item.getId() + " failed.");
		}
			
	}
	*/
	
}
