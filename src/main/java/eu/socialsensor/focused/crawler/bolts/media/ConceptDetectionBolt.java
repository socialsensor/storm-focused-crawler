package eu.socialsensor.focused.crawler.bolts.media;

import static backtype.storm.utils.Utils.tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.mathworks.toolbox.javabuilder.MWException;

import eu.socialsensor.focused.crawler.models.ImageVector;
import eu.socialsensor.framework.common.domain.Concept;
import eu.socialsensor.framework.common.domain.Concept.ConceptType;
import eu.socialsensor.framework.common.domain.MediaItem;
import gr.iti.mklab.detector.examples.Example;
import gr.iti.mklab.detector.examples.ReadFileToStringList;
import gr.iti.mklab.detector.smal.ConceptDetector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ConceptDetectionBolt extends BaseRichBolt {

	/**
	 *	@author Manos Schinas - manosetro@iti.gr
	 *
	 *	Storm Bolt using to detect concepts in media items. The bolt collects Visual Features 
	 *	of Media Items, and periodically runs a Concept Detection method.   
	 * 	
	 * 	For more information in Concept detection algorithm used from ConceptDetectionBolt
	 *  check the following project in github: 
	 *  https://github.com/socialsensor/mm-concept-detection-experiments
	 *  
	 */
	private static final long serialVersionUID = 8098257892768970548L;
	private static Logger _logger;
	
	private String matlabFile;
	private BlockingQueue<Pair<ImageVector, MediaItem>> queue;
	
	private Thread conceptDetectionThread;
	private ConceptDetectionTask conceptDetectionTask;
	private OutputCollector _collector;
	
	private ConceptDetector _detector = null;
	
	
	public ConceptDetectionBolt(String matlabFile) throws Exception {
		this.matlabFile = matlabFile;
	}
	
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		_logger = Logger.getLogger(ConceptDetectionBolt.class);
		
		_collector = collector;
		
		queue = new LinkedBlockingQueue<Pair<ImageVector, MediaItem>>();
		try {
			
			_detector = new ConceptDetector(matlabFile);
			
			conceptDetectionTask = new ConceptDetectionTask(queue);
			conceptDetectionThread = new Thread(conceptDetectionTask);
			conceptDetectionThread.start();
		}
		catch(Exception e) {
			_logger.fatal(e);
		}
	}

	public void execute(Tuple input) {
		try {
			ImageVector imgVec = (ImageVector) input.getValueByField("ImageVector");
			MediaItem mediaItem = (MediaItem) input.getValueByField("MediaItem");
			
			if(imgVec == null) {
				synchronized(_collector) {
					_collector.emit(tuple(mediaItem));
				}
			}
			else {
				queue.put(Pair.of(imgVec, mediaItem));
			}
		}
		catch(Exception e) {
			_logger.error(e);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("MediaItem"));
	}
	
	
	public class ConceptDetectionTask implements Runnable {

		private ConceptType[] conceptValues = ConceptType.values();
		private BlockingQueue<Pair<ImageVector, MediaItem>> queue;
		
		private long defaultPeriod = 15; // Run every 15 seconds
		
		public ConceptDetectionTask(BlockingQueue<Pair<ImageVector, MediaItem>> queue) {
			this.queue = queue;
		}
		
		public ConceptDetectionTask(BlockingQueue<Pair<ImageVector, MediaItem>> queue, int period) {
			this.queue = queue;
			defaultPeriod = period;
		}
		
		public void run() {
			while(true) {
				try {
					Thread.sleep(defaultPeriod * 1000);
				} catch (InterruptedException e) {
					_logger.error("ConceptDetectionTask failed to sleep. Stop execution.", e);
					break;
				}
				
				// Concept Detector needs 50 media items at least
				int n = queue.size();
				if(n < 100)  {
					_logger.info("Queue size is less than 100 images(" + n + "). Wait some more time.");
					continue;
				}
				
				List<Pair<ImageVector, MediaItem>> mediaPairs = new ArrayList<Pair<ImageVector, MediaItem>>();
				synchronized(queue) {
					queue.drainTo(mediaPairs, 1000);
				}
				
				if(mediaPairs.isEmpty()) {
					_logger.info("Queue is empty!!!");
					continue;
				}
				else {
					_logger.info("Start concept detection for " + mediaPairs.size() + " media items");
					_logger.info(queue.size() + " tuples remain to queue.");
				}
				
				long t0 = System.currentTimeMillis();
				try {
					
					Map<String, MediaItem> mediaItemsMap = new HashMap<String, MediaItem>();
					String[]   mediaIds = new String[mediaPairs.size()];
					double[][] descriptors = new double[mediaPairs.size()][];
					int k = 0;
					for(Pair<ImageVector, MediaItem> pair : mediaPairs) {
						ImageVector imageVector = pair.getLeft();
						descriptors[k] = imageVector.v;
						mediaIds[k] = imageVector.id;		
						
						MediaItem mediaItem = pair.getRight();
						mediaItemsMap.put(mediaItem.getId(), mediaItem);
						k++;
					}
					
					_logger.info("Run concept detection...");
					double[][] concepts = _detector.detect(descriptors);
					_logger.info("Done!");
					
					Map<String, Integer> temp = new HashMap<String, Integer>();
					for(int i=0; i<mediaIds.length; i++) {
						try {
							String mediaId = mediaIds[i];
							int conceptIndex = (int) concepts[i][0];
							double score = concepts[i][1];
							
							if(conceptIndex<1 || conceptIndex>7) {
								_logger.error("Condept Index (" + conceptIndex + ") out of bounds for " + mediaId);
								continue;
							}
							
							ConceptType conceptType = null;
							if(conceptIndex==2 || conceptIndex==3 || conceptIndex==4) {
								conceptType = ConceptType.heavytext;
							}
							else {
								conceptType = conceptValues[conceptIndex-1];	
							}
							
							Integer f = temp.get(conceptType.toString());
							if(f == null)
								f = 0;
							temp.put(conceptType.toString(), ++f);
							
							Concept concept = new Concept(conceptType, score);
							MediaItem mediaItem = mediaItemsMap.remove(mediaId);
							if(mediaItem != null) {
								mediaItem.addConcept(concept);
								
								synchronized(_collector) {
									_collector.emit(tuple(mediaItem));
								}
							}
						}
						catch(Exception e) {
							_logger.error("Error for media item " + mediaIds[i], e);
							continue;
						}
					}
					mediaItemsMap.clear();
					
					_logger.info("Concepts frequencies: ");
					for(Entry<String, Integer> e : temp.entrySet())
						_logger.info(e.getKey() + " => " + e.getValue());
					
				} catch (Exception e) {
					e.printStackTrace();
					_logger.error(e);
				}	
				
				t0 = System.currentTimeMillis() - t0;
				_logger.info("Concept detection ran in " + t0 + " seconds!");
				
			}	
		}
	}
	
	
	public static void main(String...args) throws NumberFormatException, IOException, MWException {
        
		ConceptDetector detector = new ConceptDetector("/home/manosetro/git/mm-concept-detection-experiments/src/main/resources/twitter_training_params.mat");
		
		List<String> ids = ReadFileToStringList.readFileToStringList("/home/manosetro/git/mm-concept-detection-experiments/testImageIds.txt"); 
		
		double[][] descriptors = Example.readVector("/home/manosetro/git/mm-concept-detection-experiments/descriptors.txt");
		System.out.println(descriptors.length + " x " + descriptors[0].length);
		
		System.out.println("Classify images");
		double[][] concepts = detector.detect(descriptors);

		
		for (int j=0;j<concepts.length;j++){
			System.out.println(ids.get(j) + "  => " + concepts[j][0]);
		}
		
	}
}
