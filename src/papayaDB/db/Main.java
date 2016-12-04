package papayaDB.db;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import papayaDB.rest.RESTQueryInterface;

public class Main {

	private static HashMap<String, DatabaseCollection> loadCollections(DatabaseIndexManager indexManager) {
		HashMap<String, DatabaseCollection> collections = new HashMap<String, DatabaseCollection>();
		try {
			File folder = new File("collections");
			if(!folder.exists()) {
				folder.mkdir();
			}
			

			for(File file : folder.listFiles()) {
				if(file.isFile()) {
					String name = file.getName();
					String nameWithoutExtension = name.substring(0, name.length()-5);
					DatabaseCollection collection = new DatabaseCollection(nameWithoutExtension);
					collections.put(nameWithoutExtension, collection);
					indexManager.addIndexToCreate(collection, "uid");
				}
			}
		} catch (IOException e) {
			System.err.println("Error while loading collections");
		}
		return collections;
	}

	public static void main(String[] args) {
		System.setProperty("vertx.disableFileCaching", "true");

		Vertx vertx = Vertx.vertx();
		DatabaseIndexManager indexManager = new DatabaseIndexManager();
		HashMap<String, DatabaseCollection> collections = loadCollections(indexManager);


		vertx.deployVerticle(new LocalDataInterface(6666, collections, indexManager));

		DeploymentOptions options = new DeploymentOptions().setWorker(true);
		vertx.deployVerticle(new DatabaseCleaningManager(collections), options);

		vertx.deployVerticle(indexManager, options);


		vertx.deployVerticle(new RESTQueryInterface("localhost", 6666, 8080));
	}
}