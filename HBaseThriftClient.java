package hbaseHelper;

import java.lang.Object;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.thrift.generated.BatchMutation;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class HBaseThriftClient {
	protected Hbase.Client hbaseClient = null;
	private String hbaseAddr = "";
	private Integer hbasePort = 0;
	private TTransport socket = null;
	private TProtocol protocol = null;
	protected static final String CHAR_SET = "UTF-8";
	
	public HBaseThriftClient(String addr, Integer port) {
		hbaseAddr = addr;
		hbasePort = port;
		socket = new TSocket(hbaseAddr, hbasePort);
		protocol = new TBinaryProtocol(socket, true, true);	
		hbaseClient = new Hbase.Client(protocol);
	}
	
	public static void main(String[] args) throws TTransportException {
		HBaseThriftClient hbaseClient = null;
		try {
			hbaseClient = new HBaseThriftClient("10.2.4.225", 9090);
			hbaseClient.openTransport();
			
			doSomeTest(hbaseClient);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			hbaseClient.closeTransport();
		}
	}	
	
	public static void doSomeTest(HBaseThriftClient client) throws TException {
		String tableName = "test";
		testScanTable(tableName, "", 1000, client);		
		
		String rowKey_R1 = "r1";
		Map<String, String> kvpUpdate_r1 = new HashMap<String, String>();
		kvpUpdate_r1.put("c1:cq3", "val_20150618_0920_1");
		kvpUpdate_r1.put("c3:cq3", "val_20150618_0920_1");
		kvpUpdate_r1.put("c3:cq333", "val_20150618_0920_1");
		//client.updateRow(tableName, rowKey_R1, kvpUpdate_r1);
	
		String rowKey_R2 = "r2";
		Map<String, String> kvpUpdate_r2 = new HashMap<String, String>();
		kvpUpdate_r2.put("c1:cq1", "val_201505181028_r2c1");
		kvpUpdate_r2.put("c2:cq3", "val_201505181028_r2c3_0");
		kvpUpdate_r2.put("c3:cq333", "val_201505181028_r2c3_1");
		
		Map<String, Map<String, String>> rowBatchData = new HashMap<String, Map<String, String>>();
		rowBatchData.put(rowKey_R1, kvpUpdate_r1);
		rowBatchData.put(rowKey_R2, kvpUpdate_r2);
		//client.updateRows(tableName, rowBatchData);
		Map<String, String> attributes = new HashMap<String, String>();
		//List<TRowResult> rowRslts = client.getRow("test", bytesKey.toString(), attributes);
				
		Map<String, String> kvpUpdate_bs = new HashMap<String, String>();
		kvpUpdate_bs.put("c1:cq3", "val_20150519_1352");
		kvpUpdate_bs.put("c3:cq3", "val_20150519_1352_c3_0");
		kvpUpdate_bs.put("c3:cq333", "val_2015059_1352_1");
		//client.updateRow(tableName, "r3", kvpUpdate_bs);
		
		//client.deleteCell(tableName, "r3", "c2:cq4");	
		
		List<String> columns = new ArrayList<String>();
		columns.add("c2:cq1");	
		columns.add("c2:cq3");	
		//client.deleteCells(tableName, "r3", columns);
		
		
		client.deleteRow(tableName, "r3");
		
		testScanTable(tableName, "", 1000, client);	
		System.out.println("Done.");
	}
	
	public static void testIterateRow(String tableName, String rowKey, HBaseThriftClient client) throws TException {
        Map<String, String> attributes = new HashMap<String, String>();  
		List<TRowResult> results = client.getRow(tableName, rowKey, attributes);
		for (TRowResult rslt : results) {
			client.iterateResults(rslt);
		}		
	}
	
	public static void testScanTable(String tableName, String startRow, int rowCnt, HBaseThriftClient client) throws TException {		
		List<String> columns = new ArrayList<String>(0);
		Map<String, String> attributesTest = new HashMap<String, String>();
		int scannerID = client.scannerOpen(tableName, startRow, columns, attributesTest);
		try {
			List<TRowResult> scanResults = client.scannerGetList(scannerID,	rowCnt);
			while (scanResults != null && !scanResults.isEmpty()) {
				for (TRowResult rslt : scanResults) {
					client.iterateResults(rslt);
				}
				scanResults = client.scannerGetList(scannerID, rowCnt);
			}
		} finally {
			client.scannerClose(scannerID);
		}
	}	
    
    public void listTableNames(HBaseThriftClient client) throws TTransportException {
		List<String> tblNames = client.getTableNames();
		for (String name : tblNames) {
			System.out.println(">> " + name);
		}
    }	
	
    public void deleteRow(String table, String rowKey) throws TException {
    	ByteBuffer tableName = getByteBuffer(table);
    	ByteBuffer row = getByteBuffer(rowKey);
    	hbaseClient.deleteAllRow(tableName, row, getAttributesMap(new HashMap<String, String>()));
    }
    
    public void deleteCell(String table, String rowKey, String column) throws TException {
    	List<String> columns = new ArrayList<String>(1);
    	columns.add(column);
    	deleteCells(table, rowKey, columns);
    }
    
    public void deleteCells(String table, String rowKey, List<String> columns) throws TException {
    	boolean writeToWal = false;
    	List<Mutation> mutations = new ArrayList<Mutation>();
    	for (String column : columns) {
    		mutations.add(new Mutation(false, getByteBuffer(column), null, writeToWal));
    	}
    	ByteBuffer tableName = getByteBuffer(table);
    	ByteBuffer row = getByteBuffer(rowKey);
    	hbaseClient.mutateRow(tableName, row, mutations, getAttributesMap(new HashMap<String, String>()));
    }
    
    public void updateRow(String table, String rowKey, Map<String, String> rowData) throws TException {	
		boolean writeToWal = false;
		Map<String, String> attributes = new HashMap<String, String>();
        List<Mutation> mutations = new ArrayList<Mutation>();
        
        for(Map.Entry<String, String> entry : rowData.entrySet()) {
            mutations.add(new Mutation(false, getByteBuffer(entry.getKey()), getByteBuffer(entry.getValue()), writeToWal));
        }
        Map<ByteBuffer, ByteBuffer> wrappedAttributes = getAttributesMap(attributes);
        ByteBuffer tableName = getByteBuffer(table);
        ByteBuffer row = getByteBuffer(rowKey);
        hbaseClient.mutateRow(tableName, row, mutations, wrappedAttributes);
    }
    
	public void updateRows(String table, Map<String, Map<String, String>> rowBatchData) throws TException {
		boolean writeToWal = false;
		Map<String, String> attributes = new HashMap<String, String>();
		Map<ByteBuffer, ByteBuffer> wrappedAttributes = getAttributesMap(attributes);
		ByteBuffer tableNameByte = getByteBuffer(table);
		List<BatchMutation> rowBatches = new ArrayList<BatchMutation>();
		
        for(Entry<String, Map<String, String>> batchEntry : rowBatchData.entrySet()) {
        	List<Mutation> mutations = new ArrayList<Mutation>();
        	for (Map.Entry<String, String> rowData : batchEntry.getValue().entrySet()) {
        		mutations.add(new Mutation(false, getByteBuffer(rowData.getKey()), getByteBuffer(rowData.getValue()), writeToWal));
        	}
        	BatchMutation batchMutation = new BatchMutation(getByteBuffer(batchEntry.getKey()), mutations);
        	rowBatches.add(batchMutation);
        }
        hbaseClient.mutateRows(tableNameByte, rowBatches, wrappedAttributes);
    }
       
    public void iterateResults(TRowResult result) {
        Iterator<Entry<ByteBuffer, TCell>> iter = result.columns.entrySet().iterator();
        System.out.println("RowKey:" + new String(result.getRow()));
        while (iter.hasNext()) {
            Entry<ByteBuffer, TCell> entry = iter.next();
            System.out.println("\tCol=" + new String(toBytes(entry.getKey())) + ", Value=" + new String(entry.getValue().getValue()));
        }
    }
    
	public List<TRowResult> scannerGetList(int id, int nbRows)throws TException {
		return hbaseClient.scannerGetList(id, nbRows);
	}

	public List<TRowResult> scannerGet(int id) throws TException {
		return hbaseClient.scannerGetList(id, 1);
	}  
    
    public int scannerOpen(String table, String startRow, String stopRow, List<String> columns,  Map<String, String> attributes) throws TException {
        ByteBuffer tableName = getByteBuffer(table);
        List<ByteBuffer> blist = getColumnsByte(columns);
        Map<ByteBuffer, ByteBuffer> wrappedAttributes = getAttributesMap(attributes);
        return hbaseClient.scannerOpenWithStop(tableName, getByteBuffer(startRow), getByteBuffer(stopRow), blist, wrappedAttributes);
    }
  
    public int scannerOpen(String table, String startRow, List<String> columns, Map<String, String> attributes) throws TException {
        ByteBuffer tableName = getByteBuffer(table);
        List<ByteBuffer> blist = getColumnsByte(columns);
        Map<ByteBuffer, ByteBuffer> wrappedAttributes = getAttributesMap(attributes);
        return hbaseClient.scannerOpen(tableName, getByteBuffer(startRow), blist, wrappedAttributes);
    }
    
	public void scannerClose(int id) throws TException {
		hbaseClient.scannerClose(id);
	}

	public List<ByteBuffer> getColumnsByte(List<String> columns) { 	
        List<ByteBuffer> blist = new ArrayList<ByteBuffer>();	
        for(String column : columns) {	
        	blist.add(getByteBuffer(column));	
        }    	
        return blist;   	
    }
 
	protected byte[] toBytes(ByteBuffer buffer) {
		byte[] bytes = new byte[buffer.limit()];
		for (int i = 0; i < buffer.limit(); i++) {
			bytes[i] = buffer.get();
		}
		return bytes;
	}
      
	public List<TRowResult> getRow(String table, String row, Map<String, String> attributes) throws TException {
        ByteBuffer tableName = getByteBuffer(table);
        Map<ByteBuffer, ByteBuffer> wrappedAttributes = getAttributesMap(attributes);
        return hbaseClient.getRow(tableName, getByteBuffer(row), wrappedAttributes);
    }
	
	public List<String> getTableNames() throws TTransportException {
		ArrayList<String> tableNames = new ArrayList<String>();
		try {
			for (ByteBuffer name : hbaseClient.getTableNames()) {
				tableNames.add(byteBufferToString(name));
			}			
			return tableNames;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		} 
	}
	
	private static Map<ByteBuffer, ByteBuffer> getAttributesMap(Map<String, String> attributes) {
        Map<ByteBuffer, ByteBuffer> attributesMap = null;
        if(attributes != null && !attributes.isEmpty()) {
        	attributesMap = new HashMap<ByteBuffer, ByteBuffer>();
            for(Map.Entry<String, String> entry : attributes.entrySet()) {
            	attributesMap.put(getByteBuffer(entry.getKey()), getByteBuffer(entry.getValue()));
            }
        }
        return attributesMap;
    }
	
	public static String byteBufferToString(ByteBuffer buffer) {
		CharBuffer charBuffer = null;
		try {
			Charset charset = Charset.forName(CHAR_SET);
			CharsetDecoder decoder = charset.newDecoder();
			charBuffer = decoder.decode(buffer);
			buffer.flip();
			return charBuffer.toString();
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
	}

	public static ByteBuffer getByteBuffer(String str) {
		return ByteBuffer.wrap(str.getBytes());
	}
	
	public void openTransport() throws TTransportException {
		if (socket != null) {
		    socket.open();		
		}
	}

	public void closeTransport() throws TTransportException {
		if (socket != null) {
    		socket.close();
		}
	}	
}
