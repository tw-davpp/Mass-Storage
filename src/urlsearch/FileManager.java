package urlsearch;

import java.io.File;

public class FileManager {
	public static void checkOutputFile(File file) {
		if (file.exists()) {
			delete(file);
		}
	}

	private static void delete(File file) {
		if (!file.isDirectory())
			file.delete();
		else {
			for (File f : file.listFiles()) {
				delete(f);
			}
			file.delete();
		}
	}
}
