package eu.socialsensor.focused.crawler.bolts.media;

import static backtype.storm.utils.Utils.tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
	private static Logger logger;
	
	private String matlabFile;
	private BlockingQueue<Pair<ImageVector, MediaItem>> queue;
	private Thread thread;
	private DetectionTask task;
	private OutputCollector _collector;
	
	private ConceptDetector detector = null;
	
	
	public ConceptDetectionBolt(String matlabFile) throws Exception {
		this.matlabFile = matlabFile;
	}
	
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		logger = Logger.getLogger(ConceptDetectionBolt.class);
		
		_collector = collector;
		
		queue = new LinkedBlockingQueue<Pair<ImageVector, MediaItem>>();
		try {
			
			detector = new ConceptDetector(matlabFile);
			
			task = new DetectionTask(queue);
			thread = new Thread(task);
			thread.start();
		}
		catch(Exception e) {
			logger.fatal(e);
		}
	}

	public void execute(Tuple input) {
		try {
			ImageVector imgVec = (ImageVector) input.getValueByField("ImageVector");
			MediaItem mediaItem = (MediaItem) input.getValueByField("MediaItem");
			
			if(imgVec == null) {
				_collector.emit(tuple(mediaItem));
			}
			else {
				queue.add(Pair.of(imgVec, mediaItem));
			}
		}
		catch(Exception e) {
			logger.error(e);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("MediaItem"));
	}
	
	
	public class DetectionTask implements Runnable {

		private ConceptType[] conceptValues = ConceptType.values();
		private BlockingQueue<Pair<ImageVector, MediaItem>> queue;
		
		private int defaultPeriod = 10 * 60; // 10 minutes
		
		public DetectionTask(BlockingQueue<Pair<ImageVector, MediaItem>> queue) {
			this.queue = queue;
		}
		
		public DetectionTask(BlockingQueue<Pair<ImageVector, MediaItem>> queue, int period) {
			this.queue = queue;
			defaultPeriod = period;
		}
		
		public void run() {
			while(true) {
				try {
					Thread.sleep(defaultPeriod * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
					break;
				}
				
				List<Pair<ImageVector, MediaItem>> mediaPairs = new ArrayList<Pair<ImageVector, MediaItem>>();
				queue.drainTo(mediaPairs, 1500);
				
				if(mediaPairs.isEmpty()) {
					logger.info("Queue is empty! ");
					continue;
				}
				else {
					logger.info("Start concept detection for " + mediaPairs.size() + " media items");
					logger.info(queue.size() + " tuples remain to queue.");
				}
				
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
					
					logger.info("Run concept detection....");
					double[][] concepts = detector.detect(descriptors);
					logger.info("Done!");
					
					for(int i=0; i<mediaIds.length; i++) {
						try {
							String mediaId = mediaIds[i];
							int conceptIndex = (int) concepts[i][0];
							double score = concepts[i][1];
							
							if(conceptIndex<1 || conceptIndex>9)
								continue;
							
							ConceptType conceptType = conceptValues[conceptIndex-1];
						
							Concept concept = new Concept(conceptType, score);
							MediaItem mediaItem = mediaItemsMap.remove(mediaId);
							if(mediaItem != null) {
								mediaItem.addConcept(concept);
								_collector.emit(tuple(mediaItem));
							}
						}
						catch(Exception e) {
							e.printStackTrace();
							logger.error(e);
							continue;
						}
					}
					mediaItemsMap.clear();
				} catch (Exception e) {
					e.printStackTrace();
					logger.error(e);
				}	
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
