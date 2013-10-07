package eu.socialsensor.focused.crawler.utils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import eu.socialsensor.framework.common.domain.MediaItem;

public class VideoExtractor {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws MalformedURLException 
	 */
	public static void main(String[] args) throws MalformedURLException, IOException {
		String url = "http://www.bbc.co.uk/news/world-europe-24104741";

		String content = IOUtils.toString(new URL(url).openStream());
		
		Document doc = Jsoup.parse(content);
		
		Elements iframes = doc.select("iframe");
		String videoURL = iframes.attr("src");
		System.out.println(iframes.size());
		System.out.println(videoURL);

		
		Elements videos = doc.select("videos");
		videoURL = videos.attr("src");
		System.out.println(videos.size());
		System.out.println(videoURL);
		
		Elements embed = doc.select("embed");
		videoURL = embed.attr("src");
		System.out.println(embed.size());
		System.out.println(videoURL);
		
	}

	public List<MediaItem> extractVideos(String html) {
		List<MediaItem> videos = new ArrayList<MediaItem>();
		
		// TODO: Add code to extract <object> and <embed> tags
		
		return videos;
	}
}
