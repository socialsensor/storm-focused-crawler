package eu.socialsensor.focused.crawler.items;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import eu.socialsensor.focused.crawler.utils.Snapshots;
import eu.socialsensor.focused.crawler.utils.Vocabulary;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class EventDetectionBolt extends BaseRichBolt {

	private Snapshots<Vocabulary> _vocabularySnapshots;
	private Snapshots<Map<String, Double>> _idfShiftsSnapshots;
	
	private int _windows;
	private long _windowLength;

	private Vocabulary _currentVocabulary;

	private Logger _logger;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3868431827526927531L;

	public EventDetectionBolt(int windows, long windowLength) {
		_windows = windows;
		_windowLength = windowLength;
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_logger = Logger.getLogger(EventDetectionBolt.class);
		
		_vocabularySnapshots = new Snapshots<Vocabulary>(_windows);
		_idfShiftsSnapshots = new Snapshots<Map<String, Double>>(_windows-1);
		
		_currentVocabulary = new Vocabulary();
		
		Thread thread = new Thread(new EventDetector());
		thread.start();
	}

	@Override
	public void execute(Tuple input) {
		try {
			@SuppressWarnings("unchecked")
			List<String> tokens = (List<String>) input.getValueByField("tokens");
			_currentVocabulary.addWords(tokens);
		}
		catch(Exception e) {
			_logger.error(e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	private class EventDetector implements Runnable {

		@Override
		public void run() {
			long t = 0;
			while(true) {
				try {
					Thread.sleep(Math.max(_windowLength * 1000 - t, 0));
				} catch (InterruptedException e) {
					_logger.error("event detector thread interrupted. ", e);
					break;
				}
				
				t = System.currentTimeMillis();
				
				Vocabulary previousVocabulary = _vocabularySnapshots.getLast();
				Map<String, Double> idfShift = _currentVocabulary.getShift(previousVocabulary);
				
				// TODO: Detect Event Candidates
				
				_vocabularySnapshots.add(_currentVocabulary);
				_idfShiftsSnapshots.add(idfShift);
				
				_currentVocabulary = new Vocabulary();
				
				t = System.currentTimeMillis() - t;
				
			}
			
		}
	}
}
