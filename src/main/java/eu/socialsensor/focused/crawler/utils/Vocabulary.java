package eu.socialsensor.focused.crawler.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class Vocabulary {
	
	private long numOfDocs = 0;
		
	private Map<String, Long> voc = new TreeMap<String, Long>(); 

	public void clear() {
		numOfDocs = 0;
		voc.clear();
	}
	
	public void addWords(List<String> words) {
		numOfDocs++;
		for(String word : words) {
			Long frequency = voc.get(word);
			if(frequency == null)
				frequency = 1L;
			else
				frequency += 1;
			voc.put(word, frequency);	
		}
	}
	
	public double getDf(String word) {
		Long df = voc.get(word);
		if(df == null) {
			return 0;
		}
		
		return (double)df / (double)numOfDocs;
	}
	
	public double getIdf(String word) {
		Long df = voc.get(word);
		if(df == null) {
			return 0;
		}
		
		return Math.log10((double)numOfDocs / (double)df);
	}

	public int size() {
		return voc.size();
	}
	
	public Map<String, Double> getShift(Vocabulary other) {
		Map<String, Double> idfShifts = new HashMap<String, Double>();
		
		Set<String> set = new HashSet<String>();
		set.addAll(other.voc.keySet());
		
		set.retainAll(this.voc.keySet());
		
		for(String word : set) {
			double idf1 = this.getIdf(word);
			double idf2 = other.getIdf(word);
			
			double sidf = (idf2 - idf1)/idf1;
			idfShifts.put(word, sidf);
		}
		
		return idfShifts;
	}
}