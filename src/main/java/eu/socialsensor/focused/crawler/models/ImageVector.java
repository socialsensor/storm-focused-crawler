package eu.socialsensor.focused.crawler.models;

public class ImageVector {
	public String id;
	public double[] v;
	public String url;

	public ImageVector(String id, String url, double[] v) {
		this.id = id;
		this.url = url;
		this.v = v;
	}
}
