package papayaDB.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import io.vertx.core.Vertx;
import papayaDB.rest.RESTQueryInterface;

public class Main {
	
	private static Map<String, DatabaseCollection> loadCollections() {
		Map<String, DatabaseCollection> collections = new HashMap<String, DatabaseCollection>();
		try {
			collections.put("testDb", new DatabaseCollection("testDb"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return collections;
	}
	
	public static void main(String[] args) {
		System.setProperty("vertx.disableFileCaching", "true");
	    
	    Vertx vertx = Vertx.vertx();
	    
	    Map<String, DatabaseCollection> collections = loadCollections();
	    vertx.deployVerticle(new LocalDataInterface(6666, collections));
	    
	    
	    
	    vertx.deployVerticle(new RESTQueryInterface("localhost", 6666, 8080));
	}
}