package eu.socialsensor.focused.crawler.bolts.media;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;

import javax.imageio.ImageIO;

import org.apache.commons.io.IOUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import eu.socialsensor.framework.client.search.visual.JsonResultSet;
import eu.socialsensor.framework.client.search.visual.JsonResultSet.JsonResult;
import eu.socialsensor.framework.client.search.visual.VisualIndexHandler;
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
	
	private OutputCollector _collector;
	private VisualIndexHandler visualIndex;

	private String webServiceHost;
	private String indexCollection;

	private static int[] numCentroids = { 128, 128, 128, 128 };
	private static int targetLengthMax = 1024;
	
	private static int maxNumPixels = 768 * 512; // use 1024*768 for better/slower extraction
	
	public VisualIndexerBolt(String webServiceHost, String indexCollection, String[] codebookFiles, String pcaFile) throws Exception {
		
		this.webServiceHost = webServiceHost;
		this.indexCollection = indexCollection;
		
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
		this._collector = collector;
		this.visualIndex = new VisualIndexHandler(webServiceHost, indexCollection);
	}

	public void execute(Tuple tuple) {
		
		String id = tuple.getStringByField("id");
		String url = tuple.getStringByField("url");
		//Double score = tuple.getDoubleByField("score");
		
		boolean size = tuple.getBooleanByField("size");
		
		//System.out.println("Fetch and extract feature vector for " + id + " with score " + score);
		try {
			
			byte[] imageContent = IOUtils.toByteArray(new URL(url));
			BufferedImage image = ImageIO.read(new ByteArrayInputStream(imageContent));
			
			
			Integer width=-1, height=-1;
			boolean indexed = false;
			
			if(image != null) {
				
				ImageVectorization imvec = new ImageVectorization(id, image, targetLengthMax, maxNumPixels);
				
				if(!size) {
					width = image.getWidth();
					height = image.getHeight();
				}

				ImageVectorizationResult imvr = imvec.call();
				double[] vector = imvr.getImageVector();

				
				indexed = visualIndex.index(id, vector);
	
			}
			
			if(indexed) {
				_collector.emit(tuple(id, Boolean.TRUE, width, height));
			}
			else {
				_collector.emit(tuple(id, Boolean.FALSE, width, height));
			}
		} 
		catch (Exception e) {
			System.out.println("Exception: " + e.getMessage() + " url: "+url);
			_collector.emit(tuple(id, Boolean.FALSE, -1, -1));
			return;
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "indexed", "width", "height"));
	}

	public static void main(String[] args) throws Exception {
		cluster();
	}
	
	public static void cluster() throws Exception {
		
		String learningFiles = "/disk2_data/VisualIndex/learning_files/";
		
		String[] codebookFiles = { 
				learningFiles + "surf_l2_128c_0.csv",
				learningFiles + "surf_l2_128c_1.csv", 
				learningFiles + "surf_l2_128c_2.csv",
				learningFiles + "surf_l2_128c_3.csv" };
		
		String pcaFile = learningFiles + "pca_surf_4x128_32768to1024.txt";
		
		ImageVectorization.setFeatureExtractor(new SURFExtractor());

		VladAggregatorMultipleVocabularies vladAggregator = new VladAggregatorMultipleVocabularies(codebookFiles, numCentroids, AbstractFeatureExtractor.SURFLength);
		
		ImageVectorization.setVladAggregator(vladAggregator);
		
		int initialLength = numCentroids.length * numCentroids[0] * AbstractFeatureExtractor.SURFLength;
		if(initialLength > targetLengthMax) {
			PCA pca = new PCA(targetLengthMax, 1, initialLength, true);
			pca.loadPCAFromFile(pcaFile);
			ImageVectorization.setPcaProjector(pca);
		}
		
		String mongoHost = "160.40.51.18";
		String mongoDb = "FeteBerlin";
		String mongoCollection = "MediaItems";
		
		MongoClient client = new MongoClient(mongoHost);
		DB db = client.getDB(mongoDb);
		DBCollection coll = db.getCollection(mongoCollection);
		DBCollection clustersCollection = db.getCollection("MediaItemClusters3");
				
		VisualIndexHandler vIndex = new VisualIndexHandler("http://160.40.51.18:8080/VisualIndexService", "fete3");
		
		DBCursor cursor = coll.find(new BasicDBObject("type","image"));
		
		ArrayBlockingQueue<Vec> queue1 = new ArrayBlockingQueue<Vec>(5000);
		ArrayBlockingQueue<Vec> queue2 = new ArrayBlockingQueue<Vec>(5000);
		
		Thread clusterer = new Thread(new Clusterer(queue2, vIndex, coll, clustersCollection));
		List<Thread> extractors = new ArrayList<Thread>();
		for(int i=0;i<48;i++) {
			extractors.add(new Thread(new Extractor(queue1, queue2)));
		}
		
		clusterer.start();
		for(Thread t : extractors) {
			t.start();
		}
		while(cursor.hasNext()) {
			
			DBObject mItem = cursor.next();
			String id = (String) mItem.get("id");
			String url = (String) mItem.get("url");
				
			try {
				Vec vec = new Vec(id, url, null);
				queue1.put(vec);
				Thread.sleep(5);
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		System.out.println("Wait for clusterer to finish!!!");
		clusterer.join();
	}
	
	public static class Extractor implements Runnable {
		ArrayBlockingQueue<Vec> queue1, queue2;
		
		public Extractor(ArrayBlockingQueue<Vec> queue1, ArrayBlockingQueue<Vec> queue2) {
			this.queue1 = queue1;
			this.queue2 = queue2;

		}
		
		public void run() {
			while(true) {
				Vec vec = queue1.poll();
				if(vec==null) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					continue;
				}
				
				String id = vec.id;
				String url = vec.url;
				
				BufferedImage image;
				try {
					image = ImageIO.read(new URL(url));
					
					ImageVectorization imvec = new ImageVectorization(id, image, targetLengthMax, maxNumPixels);
					
					ImageVectorizationResult imvr = imvec.call();
					double[] vector = imvr.getImageVector();
					
					vec.v = vector;
					
					queue2.put(vec);
					
				} catch (Exception e) {
					//e.printStackTrace();
					//System.out.println(e.getMessage());
					continue;
				} 
				
				
			}
			
		}
		
	}
	
	public static class Clusterer implements Runnable {

		private ArrayBlockingQueue<Vec> queue;
		private VisualIndexHandler vIndex;
		private DBCollection coll, clustersCollection;

		public Clusterer(ArrayBlockingQueue<Vec> queue, VisualIndexHandler vIndex, DBCollection coll, DBCollection clustersCollection) {
			this.queue = queue;
			this.vIndex = vIndex;
			
			this.coll = coll;
			this.clustersCollection = clustersCollection;
		}
		
		public void run() {
			int k=0;
			while(true) {
				
				k++;
				if(k % 100 == 0)
					System.out.println(k + " indexed!");
				
			Vec vec = queue.poll();
			if(vec==null) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				continue;
			}
			
			String id = vec.id;
			double[] vector = vec.v;
			
			JsonResultSet similar = vIndex.getSimilarImages(vector, 0.95);
			List<JsonResult> results = similar.getResults();
			if(results.size()>0) {
				String nearestId = results.get(0).getId();
				//System.out.println(id + " -> " +nearestId);
			
				DBObject obj = coll.findOne(new BasicDBObject("id", nearestId));
				if(obj != null && obj.containsField("clusterId")) {
					
					// Add media item to the same cluster as the nearest neighbor
					String clusterId = (String) obj.get("clusterId");
					
					System.out.println("Add " + id + " in cluster " + clusterId + " Nearest: " + nearestId);
					
					coll.update(new BasicDBObject("id", id), new BasicDBObject("$set", new BasicDBObject("clusterId", clusterId)));
					
					BasicDBObject cluster = new BasicDBObject("id", clusterId);
					DBObject update = new BasicDBObject("$addToSet", new BasicDBObject("members", id));
					update.put("$inc", new BasicDBObject("count", 1));
					clustersCollection.update(cluster, update);
				}
				else {
					if(obj == null)
						System.out.println("Error: " + nearestId + " not found!");
					else
						System.out.println("Error: " + nearestId + " not clustered!");
				}
			}
			else {
				// Create new Cluster
				UUID clusterId = UUID.randomUUID();
				
				coll.update(new BasicDBObject("id", id), new BasicDBObject("$set", new BasicDBObject("clusterId", clusterId.toString())));
				
				BasicDBObject cluster = new BasicDBObject("id", clusterId.toString());
				
				List<String> members = new ArrayList<String>();
				members.add(id);
				cluster.put("members", members);
				cluster.put("count", 1);
				clustersCollection.insert(cluster);
			}
			
			boolean indexed = vIndex.index(id, vector);
			if(indexed)
				coll.update(new BasicDBObject("id", id), new BasicDBObject("$set", new BasicDBObject("indexed", indexed)));
			
			
			
			}
		}
		
	}

	private static class Vec {
		public String id;
		public double[] v;
		public String url;

		Vec(String id, String url, double[] v) {
			this.id = id;
			this.url = url;
			this.v = v;
		}
	}
}
