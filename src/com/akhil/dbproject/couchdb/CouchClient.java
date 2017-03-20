package com.akhil.dbproject.couchdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.lightcouch.CouchDbClient;
import org.lightcouch.CouchDbProperties;
import org.lightcouch.Document;
import org.lightcouch.View;

import com.google.gson.JsonObject;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

public class CouchClient extends DB {

	private CouchDbClient dbClient;
	private int batchSize;
	private List<JsonObject> batchInsertList;

	@Override
	public void init() throws DBException {
		CouchDbProperties properties = new CouchDbProperties();
		//CouchDB host IP
		properties.setHost("127.0.0.1");
		//CouchDB port - default is 5984
		properties.setPort(5984);
		//CouchDB database name
		properties.setDbName("testdb");
		//Also set username and password here if required
		properties.setCreateDbIfNotExist(true);
		properties.setProtocol("http");
		Properties props = getProperties();
		batchInsertList = new ArrayList<JsonObject>();
		//batchsize is used in case of insertions
		batchSize = Integer.parseInt(props.getProperty("batchsize", "10000"));
		dbClient = new CouchDbClient(properties);
		super.init();
	}

	@Override
	public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
		JsonObject found = dbClient.find(JsonObject.class, key, "stale=ok");
		if (null == found)
			return Status.NOT_FOUND;
		if (fields != null) {
			JsonObject jsonObject = new JsonObject();
			jsonObject.add("_id", found.get("_id"));
			jsonObject.add("_rev", found.get("_rev"));
			for (String field : fields) {
				jsonObject.add(field, found.get(field));
			}
			result.put(found.get("_id").toString(), new ByteArrayByteIterator(jsonObject.toString().getBytes()));
		}
		return Status.OK;
	}

	@Override
	public Status scan(String table, String startkey, int recordcount, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result) {
		View view = dbClient.view("_all_docs").startKeyDocId(startkey).limit(recordcount).includeDocs(true);
		HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();
		List<JsonObject> list = view.query(JsonObject.class);
		if (fields != null) {
			for (JsonObject doc : list) {
				JsonObject jsonObject = new JsonObject();
				jsonObject.add("_id", doc.get("_id"));
				jsonObject.add("_rev", doc.get("_rev"));
				for (String field : fields) {
					jsonObject.add(field, doc.get(field));
				}
				resultMap.put(doc.get("_id").toString(), new ByteArrayByteIterator(jsonObject.toString().getBytes()));
			}
			result.add(resultMap);
		}
		for (HashMap<String, ByteIterator> map : result) {
			for (String key : map.keySet()) {
				System.out.println(map.get(key).toString());
			}
		}
		return Status.OK;
	}

	@Override
	public Status update(String table, String key, HashMap<String, ByteIterator> values) {
		JsonObject jsonObject = dbClient.find(JsonObject.class, key);
		if (null == jsonObject) {
			return Status.NOT_FOUND;
		}
		for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
			jsonObject.addProperty(entry.getKey(), entry.getValue().toString());
		}
		dbClient.update(jsonObject);
		return Status.OK;
	}

	@Override
	public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
		JsonObject jsonObject = new JsonObject();
		jsonObject.addProperty("_id", key);
		for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
			jsonObject.addProperty(entry.getKey(), entry.getValue().toString());
		}
		if (batchSize == 1) {
			dbClient.save(jsonObject);
		} else {
			batchInsertList.add(jsonObject);
			if (batchInsertList.size() == batchSize) {
				dbClient.bulk(batchInsertList, false);
				batchInsertList.clear();
			}
		}
		return Status.OK;
	}

	@Override
	public Status delete(String table, String key) {
		dbClient.remove(dbClient.find(Document.class, key));
		return Status.OK;
	}

}
