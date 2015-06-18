package hbaseHelper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

public class HBaseHelper {
	private static Integer threadCount = 10;
	private static Integer tCnt = 0;
	private static String runtimeDir = System.getProperty("user.dir");
	private static String logFolder = runtimeDir + "\\logfolder";
	private static String mapFileStr = logFolder + "\\pathmap.txt";
	private static File logFile = new File(logFolder + "\\log.txt");
	private static File[] pathArray = null;
	private static BufferedWriter logBW = null;
	private static HashMap<File, Integer> pathMap = new HashMap<File, Integer>();
	private static ExecutorService executor = null;
	private static final ReentrantLock dispatchLock = new ReentrantLock();
	private static final ReentrantLock mapLock = new ReentrantLock();
	private static final ReentrantLock mwLock = new ReentrantLock();
	private static final ReentrantLock cntLock = new ReentrantLock();
	private static final ReentrantLock hbaseLock = new ReentrantLock();

	private static String rootDirStr = "C:\\works\\FileScanner\\hbase_insert";;
	
	/* for HBase */
	private static HBaseThriftClient hbaseHelper = new HBaseThriftClient("10.2.4.76", 9090);
	private static final String hbaseTable= "DFIS_FileProperties";
	
	public static void main(String[] args) throws Exception {
		//testGetRow("D:\\NeweggPicturesCache\\etilize\\user-manual\\A\\27\\User_Manual_34-147-865CVF.pdf");
		insertMetaToHBase(args);
	}

	static void testGetRow(String fileName) throws Exception {		
		try {
			hbaseHelper.openTransport();
			String hashRowKey = getHashString(fileName);
			HBaseThriftClient.testIterateRow(hbaseTable, hashRowKey, hbaseHelper);			
		} catch (NoSuchAlgorithmException e1) {
			e1.printStackTrace();
		} finally {
			hbaseHelper.closeTransport();
		}
	}
	
	static void insertMetaToHBase(String[] args) {
		try {
			new File(logFolder).mkdir();
			
			if (logFile.exists()) {
				logFile.delete();
			}
			 
			if (args.length > 0) {
			    rootDirStr = args[0];
			}
			
			if (!initPathMap()) {
				buildPathMap(rootDirStr);
			}
			
			pathArray = new File[pathMap.size()];
			int i = 0;
			for (Map.Entry<File, Integer> kvp : pathMap.entrySet()) {
				pathArray[i] = kvp.getKey();
				i += 1;
			}
			writePathMap();
			dispatchTasks();
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	private static boolean initPathMap() throws Exception {
		BufferedReader br = null;
		try {
			File mapFile = new File(mapFileStr);
			if (!mapFile.exists()) {
				return false;
			}
			br = new BufferedReader(new FileReader(mapFile));
			String line = null;
			String delim = "[|]";
			while ((line = br.readLine()) != null) {
				if (line != "") {
					System.out.println("Got path: " + line);
					String[] dirInfo = line.split(delim);
					File dirPath = new File(dirInfo[0].trim());
					Integer val = Integer.valueOf(dirInfo[1].trim());
					pathMap.put(dirPath, val);
				}
			}
			br.close();
			return true;
		} catch (IOException ioe) {
			System.out.println(" Error! The path map file is unavailable.");
			ioe.printStackTrace();
			return false;
		} finally {
			if (br != null) {
				br.close();
			}
		}
	}
	
	private static void writePathMap() throws Exception {
		BufferedWriter newMapBW = null;
		try {
			mwLock.lock();
			File newPathMapFile = new File(mapFileStr);
			if (newPathMapFile.exists()) {
				newPathMapFile.delete();
			}
			newPathMapFile.createNewFile();
			newMapBW = new BufferedWriter(new FileWriter(newPathMapFile.getCanonicalFile(), false));
			
			for (Map.Entry<File, Integer> kvp : pathMap.entrySet()) {
				File subf = kvp.getKey();
				String dirpath = subf.getCanonicalPath();
				String content = String.format("%s|%s", dirpath, pathMap.get(subf).toString());
				newMapBW.write(content);
				newMapBW.newLine();
			}					
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			newMapBW.flush();
			newMapBW.close();
			mwLock.unlock();
		}
	}
	
	private static void buildPathMap(String startPath) throws Exception {
		File startDir = new File(startPath);
		if (startDir == null || !startDir.exists() || !startDir.canRead()
				|| (!startDir.isDirectory() && !startDir.isFile())) {
			throw new Exception(String.format("[%s] is not a valid path.", startDir.getCanonicalPath()));
		}
		buildMap(startDir);	
	}
	
	private static boolean buildMap(File thisDir) throws Exception {				
		if (thisDir.isDirectory()) {
			List<File> subPathList = new ArrayList<File>();
			try {
				subPathList = Arrays.asList(thisDir.listFiles()); 
			} catch (Exception e) {
				System.out.println("Invalid path: "	+ thisDir.getCanonicalPath());
				return false;
			}
			
			for (File subp : subPathList) {
				buildMap(subp);
			}
		} else if (thisDir.isFile()) {
			if (thisDir.getName().contains("_result")) {
				pathMap.put(thisDir, 0);
			}
		}
		return true;
	}	
 
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static void dispatchTasks() throws IOException {	
		Future[] taskHandlers = null;
		LinkedList<TaskParameter> taskWaitingList = null;
		int taskListSize = 0;		
		try {
			taskListSize = pathArray.length;
			taskWaitingList = new LinkedList<TaskParameter>();

			for (int i = 0; i < taskListSize; i++) {
				if (pathMap.get(pathArray[i]) == 0) {
					TaskParameter param = new TaskParameter();
					param.targetFile = pathArray[i];
					taskWaitingList.add(param);
				}
			}

			executor = Executors.newFixedThreadPool(threadCount);
			taskHandlers = new Future[threadCount];
			hbaseHelper.openTransport();
			while (taskWaitingList.size() > 0) {
				for (int i = 0; i < taskHandlers.length; i++) {
					if (taskHandlers[i] == null
							|| taskHandlers[i].isDone()
							|| taskHandlers[i].isCancelled()) {
						try {
							dispatchLock.lock();
							if (taskWaitingList.size() > 0) {
								taskHandlers[i] = (Future<HBaseWriter>) executor.submit(new HBaseWriter(taskWaitingList.pop(), i));
							}
						} finally {
							dispatchLock.unlock();
						}
					}
				}
			}

			executor.shutdown();
			executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
			System.out.println("Finished, total got: " + tCnt);
		} catch (Exception e) {
			e.printStackTrace();
			try {
				logBW.newLine();
				logBW.write("Error in dispatchTasks: " + e.getMessage());
				logBW.newLine();	
			} catch (IOException e1) {
				e1.printStackTrace();
			} finally {
				try {
					hbaseHelper.closeTransport();
				} catch (TTransportException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}			
			}
		} 
	} 
	
	public static String getHashString(String beHashedStr) throws NoSuchAlgorithmException {
		MessageDigest md = MessageDigest.getInstance("SHA-256");		
		md.update(beHashedStr.getBytes());
		byte[] hashBytes = md.digest();
		
		StringBuffer hexString = new StringBuffer();
		for (int i = 0; i < hashBytes.length; i++) {
			if ((0xff & hashBytes[i]) < 0x10) {
				hexString.append("0" + Integer.toHexString((0xFF & hashBytes[i])));
			} else {
				hexString.append(Integer.toHexString(0xFF & hashBytes[i]));
			}
		}
		return hexString.toString();
	}

	static class HBaseWriter implements Runnable {
		File targetFile = null;
		Integer handlerID = 0;

		HBaseWriter(TaskParameter param, int tIdx) {
			targetFile = param.targetFile;
			handlerID = tIdx;
		}

		public void run() {
			String tID = handlerID.toString();
			System.out.println("<" + tID + "> is running on " + "[" + targetFile + "]");
			List<FileInfoEntity> entities = new ArrayList<FileInfoEntity>();			
			try {
				entities = ParseFileInfo(targetFile);
				
				try {
				    hbaseLock.lock();	
					writeToHBase(entities);
				} finally {
					hbaseLock.unlock();
				}
				
				try {
					mapLock.lock();
					pathMap.put(targetFile, 1);
					writePathMap();
				} finally {
					mapLock.unlock();
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			} 
		}
		
		private void writeToHBase(List<FileInfoEntity> entities) throws NoSuchAlgorithmException, TException {
    		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z"); 
    		Map<String, Map<String, String>> rowBatchData = new HashMap<String, Map<String, String>>();		
    		
			for (FileInfoEntity ent : entities) {
				Map<String, String> rowData = new HashMap<String, String>();
				rowData.put("BS:Name", ent.fileName);
				rowData.put("BS:LastDate", ent.lastEditDate);
				rowData.put("BS:Size", ent.fileSize);
				rowData.put("BS:Path", ent.filePath);
				String rowKey = getHashString(ent.filePath);
				rowBatchData.put(rowKey, rowData);
			}			
			hbaseHelper.updateRows(hbaseTable, rowBatchData);
		}
		
//		public static String getHashString(String beHashedStr) throws NoSuchAlgorithmException {
//			MessageDigest md = MessageDigest.getInstance("SHA-256");		
//			md.update(beHashedStr.getBytes());
//			byte[] hashBytes = md.digest();
//			
//			StringBuffer hexString = new StringBuffer();
//			for (int i = 0; i < hashBytes.length; i++) {
//				if ((0xff & hashBytes[i]) < 0x10) {
//					hexString.append("0" + Integer.toHexString((0xFF & hashBytes[i])));
//				} else {
//					hexString.append(Integer.toHexString(0xFF & hashBytes[i]));
//				}
//			}
//			return hexString.toString();
//		}
		
		private List<FileInfoEntity> ParseFileInfo(File tf) throws Exception {
			BufferedReader br = null;	
			List<FileInfoEntity> entities = new ArrayList<FileInfoEntity>();
			try {
				br = new BufferedReader(new FileReader(tf));	
				String line = null;
				String delim = "[|]";			
				while ((line = br.readLine()) != null) {				
					if (line != "") {
						String[] tmpInfo = line.split(delim);	
						String filename = new File(tmpInfo[0]).getName();
						entities.add(new FileInfoEntity(filename, tmpInfo[0], tmpInfo[1], tmpInfo[2]));
						try {
							cntLock.lock();
							tCnt += 1;
						} finally {
							cntLock.unlock();
						}
					} 
 				}
				return entities;
			} catch (IOException ioe) {
				System.out.println(" Error! The file is invalid.");
				ioe.printStackTrace();
				return null;
			} finally {
				if (br != null) {
					br.close();
				}
			}
		}	
	}
	
	static class FileInfoEntity {
		public String filePath;
		public String fileName;
		public String lastEditDate;
		public String fileSize;
	
		public FileInfoEntity(String name, String path, String date, String size){
		    this.filePath = path;
		    this.fileName = name;
		    this.lastEditDate = date;		    
		    this.fileSize = size;
		}
		
		public FileInfoEntity(String[] info) {
		    this.filePath = info[1];
		    this.fileName = info[0];
		    this.lastEditDate = info[2];
		    this.fileSize = info[3];
		}
	}
	
	static class TaskParameter {
		public File targetFile = null;
	}
}
