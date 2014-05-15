package eu.socialsensor.focused.crawler.items;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import eu.socialsensor.framework.common.domain.Item;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TokenizationBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6133981740494794989L;
	
	private OutputCollector _collector = null;

	private int _minNgrams = 1, _maxNgrams = 1;
	private Logger _logger;
	
	public TokenizationBolt() {
	}
	
	public TokenizationBolt(int minNgrams, int maxNgrams) {
		_minNgrams = minNgrams;
		_maxNgrams = maxNgrams;
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		_logger = Logger.getLogger(TokenizationBolt.class);
		_collector = collector;	
	}

	@Override
	public void execute(Tuple input) {
		Item item = (Item)input.getValueByField("Item");
		if(item == null)
			return;
		
		//List<TaggedWord> taggedSentences = (List<TaggedWord>) input.getValueByField("PosTags");
		String title = item.getTitle();
		if(title != null) {
			try {
				List<String> tokens = tokenize(title);
				if(tokens != null && tokens.size()>0)
					_collector.emit(new Values(tokens));
			} catch (IOException e) {
				_logger.error(e);
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tokens"));
	}
	
	public List<String> tokenize(String text) throws IOException {

        List<String> tokens = new ArrayList<String>();
        TokenStream stream = new StandardTokenizer(Version.LUCENE_40, new StringReader(text)); 
        StopFilter stopFilter = new StopFilter(Version.LUCENE_40, stream, StandardAnalyzer.STOP_WORDS_SET);
        stopFilter.setEnablePositionIncrements(false);
        
        stream = new LowerCaseFilter(Version.LUCENE_40, stopFilter);
        
        if(_maxNgrams > 1) {
        	if(_minNgrams==1) {
        		stream = new ShingleFilter(stream, _maxNgrams);
        	
        	}
        	else {
        		ShingleFilter sf = new ShingleFilter(stream, _minNgrams, _maxNgrams);
        		sf.setOutputUnigrams(false);
        		stream = sf;
        	}
        }
        stream.reset();
        while(stream.incrementToken()) {
        	tokens.add(stream.getAttribute(CharTermAttribute.class).toString());
        }
        
        stream.close();
        return tokens;
    }  
	
}
