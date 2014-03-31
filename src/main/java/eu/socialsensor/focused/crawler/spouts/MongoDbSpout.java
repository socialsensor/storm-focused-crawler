package eu.socialsensor.focused.crawler.spouts;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import eu.socialsensor.framework.common.domain.WebPage;
import eu.socialsensor.framework.common.factories.ObjectFactory;

import static backtype.storm.utils.Utils.tuple;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class MongoDbSpout extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7261151120193254079L;
	
	private String _mongoHost;
	private String _mongoDbName;
	private String _mongoCollectionName;
	
	private SpoutOutputCollector _collector;
	
	private MongoClient _mongo = null;
	private DB _database = null;

	private DBObject _query;

	private LinkedBlockingQueue<WebPage> _queue;

	private CursorThread _listener = null;
	
	private DBCollection _collection;
	
	public MongoDbSpout(String mongoHost, String mongoDbName, String mongoCollectionName, DBObject query) {
		this._mongoHost = mongoHost;
		this._mongoDbName = mongoDbName;
		this._mongoCollectionName = mongoCollectionName;
		
		this._query = query;
	}

	
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		try {
			reset(_mongoHost, _mongoDbName, _mongoCollectionName);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		_collector = collector;
		_queue = new LinkedBlockingQueue<WebPage>(5000);
		
		try {
			_mongo = new MongoClient(_mongoHost);
			_database = _mongo.getDB(_mongoDbName);
			_collection = _database.getCollection(_mongoCollectionName);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

		_listener  = new CursorThread(_queue, _database, _mongoCollectionName, _query);
		_listener.start();
	}

	public void nextTuple() {
		
//		WebPage webPage = null;
//		try {
//			webPage = _queue.take();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//			return;
//		}
		
		WebPage webPage = _queue.poll();
		if(webPage == null) {
            Utils.sleep(100);
        } else {    	
        	synchronized(_collector) {
        		_collector.emit(tuple(webPage));
        	}
        	
    		_collection.update(
    			new BasicDBObject("url", webPage.getUrl()),
    			new BasicDBObject("$set", new BasicDBObject("status", "injected"))
    		);
        }
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("webPage"));	
	}
	
	@Override
    public void ack(Object url) {

    }

    @Override
    public void fail(Object url) {

    }
    
	class CursorThread extends Thread {

		LinkedBlockingQueue<WebPage> queue;
		String mongoCollectionName;
		DB mongoDB;
		DBObject query;
		
		public CursorThread(LinkedBlockingQueue<WebPage> queue, DB mongoDB, String mongoCollectionName, DBObject query) {
			
			this.queue = queue;
			this.mongoDB = mongoDB;
			this.mongoCollectionName = mongoCollectionName;
			this.query = query;
		}

		public void run() {
			while(true) {
				DBCursor cursor = mongoDB.getCollection(mongoCollectionName)
						.find(query).sort(new BasicDBObject("_id", -1)).limit(100);
				
				while(cursor.hasNext()) {			
					DBObject obj = cursor.next();
					WebPage webPage = ObjectFactory.createWebPage(obj.toString());
					if(webPage != null) {
						//queue.offer(webPage);
						try {
							queue.put(webPage);
							Utils.sleep(50);
						} catch (InterruptedException e) {
							Utils.sleep(100);
						}
					}
				}
				System.out.println("Injector: " + _queue.size());
				Utils.sleep(10000);
			}
		};
	}
	
	public void reset(String host, String dbName, String collectionName) throws UnknownHostException {
		MongoClient client = new MongoClient(host);
		DB db = client.getDB(dbName);
		DBCollection collection = db.getCollection(collectionName);
		
		DBObject q = new BasicDBObject("status", "injected");
		DBObject o = new BasicDBObject("$set", new BasicDBObject("status", "new"));
		collection.update(q, o, false, true);
	}
	
	
}
