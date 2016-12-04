package papayaDB.db;

import java.io.IOException;
import java.util.HashMap;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import papayaDB.rest.RESTQueryInterface;

public class Main {
	
	private static HashMap<String, DatabaseCollection> loadCollections() {
		HashMap<String, DatabaseCollection> collections = new HashMap<String, DatabaseCollection>();
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
	    
	    HashMap<String, DatabaseCollection> collections = loadCollections();
	    vertx.deployVerticle(new LocalDataInterface(6666, collections));
	    
	    DeploymentOptions options = new DeploymentOptions().setWorker(true);
	    vertx.deployVerticle(new DatabaseCleaningManager(collections), options);
	    
	    
	    vertx.deployVerticle(new RESTQueryInterface("localhost", 6666, 8080));
	}
}