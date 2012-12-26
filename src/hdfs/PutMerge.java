package hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PutMerge {
	private Path inputDir;
	private Path hdfsFile;
	private Configuration conf;
	private FileSystem hdfs;
	private FileSystem local;

	public PutMerge(String inputDir, String hdfsFile) {
		this.inputDir = new Path(inputDir);
		this.hdfsFile = new Path(hdfsFile);
	}

	public void init() throws IOException {
		conf = new Configuration();
		hdfs = FileSystem.get(conf);
		local = FileSystem.getLocal(conf);
	}

	public void merge() {
		try {
			// get local file list
			FileStatus[] inputFiles = local.listStatus(inputDir);

			// hdfs output stream
			FSDataOutputStream out = hdfs.create(hdfsFile);

			for (int i = 0; i < inputFiles.length; i++) {
				System.out.println(inputFiles[i].getPath().getName());

				// open the local input stream
				FSDataInputStream in = local.open(inputFiles[i].getPath());
				byte[] buffer = new byte[256];
				int bytesRead = 0;
				while ((bytesRead = in.read(buffer)) > 0)
					out.write(buffer, 0, bytesRead);
				in.close();
			}
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		String[] params = new String[2];
		params[0] = "/usr/hadoop-0.20.2/input";
		params[1] = "/usr/hadoop-0.20.2/source";

		PutMerge pm = new PutMerge(params[0], params[1]);
		pm.init();
		pm.merge();
	}
}
