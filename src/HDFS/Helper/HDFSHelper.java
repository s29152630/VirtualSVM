package HDFS.Helper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSHelper {
	public Configuration conf = new Configuration();

	// conf.set("fileSystem.default.name", "hdfileSystem://192.168.1.100:8020");
	// conf.set("fileSystem.hdfileSystem.impl", value);
	// conf.set("mapred.jop.tracker", "hdfileSystem://192.168.1.100:8021");

	public HDFSHelper() {
		conf.addResource(new Path(
				"hdfs://192.168.1.100:8020/etc/hadoop/conf/core-site.xml"));
		conf.addResource(new Path(
				"hdfs://192.168.1.100:8020/etc/hadoop/conf/hdfileSystem-site.xml"));
		conf.addResource(new Path(
				"hdfs://192.168.1.100:8020/etc/hadoop/conf/mapred-site.xml"));

	}

	public void HDFSUploadFile(String localFilePath, String serverDirPath) {

		Path src = new Path(localFilePath);
		// 目的位置
		Path dst = new Path(serverDirPath);
		try {
			FileSystem fileSystem = FileSystem.get(conf);

			fileSystem.copyFromLocalFile(src, dst);

			fileSystem.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void createDirectoryOnHDFS(String serverDirPath) throws Exception {

		FileSystem fileSystem = FileSystem.get(conf);
		Path dst = new Path(serverDirPath);
		fileSystem.mkdirs(dst);
		fileSystem.close();// 释放资源

	}

	public String readFileOnHDFS(String serverFilePath, int row) {
		String str = null;

		try {
			InputStream in = null;

			FileSystem fileSystem = FileSystem.get(conf);
			BufferedReader buff = null;

			Path fPath = new Path(serverFilePath);
			in = fileSystem.open(fPath);
			buff = new BufferedReader(new InputStreamReader(in));
			for (int i = 1; i < row; i++) {
				buff.readLine();
			}

			str = buff.readLine();

			buff.close();
			in.close();

			fileSystem.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

		return str;
	}

	public void downloadFileorDirectoryOnHDFS(String serverFilePath,
			String localDirPath) {
		try {
			FileSystem fileSystem = FileSystem.get(conf);
			Path path1 = new Path(serverFilePath);
			Path path2 = new Path(localDirPath);
			fileSystem.copyToLocalFile(path1, path2);
			fileSystem.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void deleteFileOnHDFS(String serverFilePath) {

		try {
			FileSystem fileSystem = FileSystem.get(conf);
			Path path = new Path(serverFilePath);
			fileSystem.deleteOnExit(path);
			fileSystem.close();// 释放资源

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public FileSystem getFileSystem() throws IOException {

		return FileSystem.get(conf);
	}
}
