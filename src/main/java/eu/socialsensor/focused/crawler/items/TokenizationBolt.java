package eu.socialsensor.focused.crawler.items;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
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

import edu.stanford.nlp.ling.TaggedWord;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.dysco.Entity;
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
	
	TokenType _type = TokenType.ALL;
	
	public TokenizationBolt(TokenType type) {
		_type = type;
	}
	
	public TokenizationBolt(int maxNgrams) {
		_maxNgrams = maxNgrams;
	}
	
	public TokenizationBolt(int minNgrams, int maxNgrams) {
		_minNgrams = minNgrams;
		_maxNgrams = maxNgrams;
	}
	
	public static enum TokenType {
		TAGS, NE, POS, ALL
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
		
		List<String> tokens = new ArrayList<String>();
		if(_type.equals(TokenType.ALL)) {
			String title = item.getTitle();
			if(title != null) {
				try {
					List<String> tk = tokenize(title);
					if(tk != null && tk.size()>0) {
						tokens.addAll(tk);
					}
				} catch (IOException e) {
					_logger.error(e);
				}
			}	
		}
		else if(_type.equals(TokenType.NE)) {
			List<Entity> entities = item.getEntities();
			if(entities != null) {
				for(Entity e : entities) {
					tokens.add(e.getName());
				}
			}
		}
		else if(_type.equals(TokenType.TAGS)) {
			String[] tags = item.getTags();
			if(tags != null && tags.length>0) {
				tokens.addAll(Arrays.asList(tags));
			}
		}
		else if(_type.equals(TokenType.POS)) {
			@SuppressWarnings("unchecked")
			List<TaggedWord> taggedWords = (List<TaggedWord>) input.getValueByField("PosTags");
			if(taggedWords != null && taggedWords.size()>0) {
				for(TaggedWord taggedWord : taggedWords) {
					String tag = taggedWord.tag();
					String word = taggedWord.word();
					
					
				}
			}
		}
		
		if(tokens.size() > 0)
			_collector.emit(new Values(tokens));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tokens"));
	}
	
	private List<String> tokenize(String text) throws IOException {

        List<String> tokens = new ArrayList<String>();
        TokenStream stream = new StandardTokenizer(Version.LUCENE_40, new StringReader(text)); 
        StopFilter stopFilter = new StopFilter(Version.LUCENE_40, stream, StandardAnalyzer.STOP_WORDS_SET);
        stopFilter.setEnablePositionIncrements(false);
        
        if(_maxNgrams > 1) {
        	ShingleFilter sf = null;
        	if(_minNgrams == 1) {
        		sf = new ShingleFilter(stopFilter, _maxNgrams);      	
        	}
        	else {
        		sf = new ShingleFilter(stopFilter, _minNgrams, _maxNgrams);
        		sf.setOutputUnigrams(false);
        	}
        	stream = new LowerCaseFilter(Version.LUCENE_40, sf);
        }
        else {
        	stream = new LowerCaseFilter(Version.LUCENE_40, stopFilter);
        }
        
        stream.reset();
        while(stream.incrementToken()) {
        	tokens.add(stream.getAttribute(CharTermAttribute.class).toString());
        }
        
        stream.close();
        return tokens;
    }  
	
}
