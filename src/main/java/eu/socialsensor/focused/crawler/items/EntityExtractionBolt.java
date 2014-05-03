package eu.socialsensor.focused.crawler.items;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.dysco.Entity;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class EntityExtractionBolt extends BaseRichBolt {

	private static final long serialVersionUID = 7935961067953158062L;

	private OutputCollector _collector;
	private Logger _logger;

	private String _serializedClassifier;
	private AbstractSequenceClassifier<CoreLabel> _classifier = null;
	
	
	public EntityExtractionBolt(String serializedClassifier) {
		this._serializedClassifier = serializedClassifier;
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector = collector;
		this._logger = Logger.getLogger(EntityExtractionBolt.class);
	    
	    try {
			_classifier = CRFClassifier.getClassifier(_serializedClassifier);
		} catch (ClassCastException e) {
			_logger.error(e);
		} catch (ClassNotFoundException e) {
			_logger.error(e);
		} catch (IOException e) {
			_logger.error(e);
		}
	    
	}

	public void execute(Tuple input) {
		try {
			Item item = (Item)input.getValueByField("Item");
			if(item == null)
				return;
			
			String title = item.getTitle();
			if(title != null) {
				List<Entity> entities = extract(title);
				item.setEntities(entities);
			}
			_collector.emit(new Values(item));
		}
		catch(Exception e){
			_logger.error(e);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Item"));
	}

	public List<Entity> extract(String text) throws Exception{
		Map<String, Entity> entities = new HashMap<String, Entity>();

		String textXML = _classifier.classifyWithInlineXML(StringEscapeUtils.escapeXml(text));

		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = dbf.newDocumentBuilder();
        
		ByteArrayInputStream bis = new ByteArrayInputStream(("<DOC>" + textXML + "</DOC>").getBytes());
		try {
			Document doc = docBuilder.parse(bis);
			
			addEntities(entities, doc, Entity.Type.PERSON);
	        addEntities(entities, doc, Entity.Type.LOCATION);
	        addEntities(entities, doc, Entity.Type.ORGANIZATION);
		} catch (Exception e) {
			_logger.error(e);
		}
        
		return new ArrayList<Entity>(entities.values());
	}

	private void addEntities(Map<String, Entity> entities, Document doc, Entity.Type tag) {
        String key;
        NodeList nodeList = doc.getElementsByTagName(tag.name());
        for (int i = 0; i < nodeList.getLength(); i++) {
            key = tag.name() + "&&&" + nodeList.item(i).getTextContent().toLowerCase();
            if (entities.containsKey(key)) {
                Entity entity = entities.get(key);
                entity.setCont(entity.getCont() + 1);
            } else {
                entities.put(key, new Entity(nodeList.item(i).getTextContent(), 1, tag));
            }
        }
    }
	
	public static void main(String[] args) throws Exception {
		
		File serializedClassifier = new File("/media/manos/Data/Libs/stanford-ner-2014-01-04/classifiers/english.all.3class.distsim.crf.ser.gz");
		
		EntityExtractionBolt bolt = new EntityExtractionBolt(serializedClassifier.toString());
		bolt.prepare(null, null, null);
		
		String text = "The fate of Lehman Brothers, the beleaguered investment bank, hung in the balance on "
				+ "Sunday as Federal Reserve officials and the leaders of major financial institutions "
				+ "continued to gather in emergency meetings trying to complete a plan to rescue the stricken bank.  "
				+ "Several possible plans emerged from the talks, held at the Federal Reserve Bank of New York "
				+ "and led by Timothy R. Geithner, the president of the New York Fed, and Treasury Secretary Henry M. Paulson Jr."
				+ " The fate of Lehman Brothers, the beleaguered investment bank, hung in the balance on "
				+ "Sunday as Federal Reserve officials and the leaders of major financial institutions "
				+ "continued to gather in emergency meetings trying to complete a plan to rescue the stricken bank.  "
				+ "Several possible plans emerged from the talks, held at the Federal Reserve Bank of New York "
				+ "and led by Timothy R. Geithner, the president of the New York Fed, and Treasury Secretary Henry M. Paulson Jr.";
		
		
		List<Entity> entities = bolt.extract(text);
		System.out.println(entities.size());
		for(Entity e : entities) {
			System.out.println(e.toJSONString());
		}
	}
	
	
}
