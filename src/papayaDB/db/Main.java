package papayaDB.db;

import java.util.Map;

import io.vertx.core.Vertx;
import papayaDB.rest.RESTQueryInterface;

public class Main {
	
	private static Map<String, DatabaseCollection> loadCollections() {
		return null;
	}
	
	public static void main(String[] args) {
		System.setProperty("vertx.disableFileCaching", "true");
	    
	    Vertx vertx = Vertx.vertx();
	    
	    Map<String, DatabaseCollection> collections = loadCollections();
	    vertx.deployVerticle(new LocalDataInterface(6666));
	    
	    
	    
	    vertx.deployVerticle(new RESTQueryInterface("localhost", 6666, 8080));
	}
}