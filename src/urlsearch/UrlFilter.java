package urlsearch;

public class UrlFilter implements Filter {
	private String url;

	public UrlFilter(String url) {
		this.url = url;
	}

	@Override
	public boolean remain(Object obj) {
		String str = (String) obj;
		if (str.equals(url))
			return true;
		else
			return false;
	}

}
