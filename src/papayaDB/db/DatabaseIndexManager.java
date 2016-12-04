package papayaDB.db;

import java.util.AbstractMap;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.stream.Stream;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;

/**
 * Cette classe représente la Thread qui s'occupe de la création d'index de base. Celle-ci récupère
 * les informations des index à créer dans les DatabaseCollection.
 */
public class DatabaseIndexManager extends AbstractVerticle {
	private boolean stopIndexing = false;
	private final ArrayBlockingQueue<Entry<DatabaseCollection, String>> indexesQueries = new ArrayBlockingQueue<>(5);
	

	@Override
	public void start() throws InterruptedException {
		buildIndexes();
	}

	public void buildIndexes() throws InterruptedException {
		while(!stopIndexing) {
			Entry<DatabaseCollection, String> indexQuery = indexesQueries.take();
			TreeMap<Comparable<?>, Integer> index = new TreeMap<>();
			
			// Pour chaque élément existant on l'ajoute en trié dans la TreeMap
			FileStorageManager storageManager = indexQuery.getKey().getStorageManager();
			Stream<Entry<Integer, Integer>> addressStream = storageManager.getRecordsMap().entrySet().stream();
			addressStream.map(entry -> {
				JsonObject doc = storageManager.getRecordAtAddress(entry.getKey());
				index.put(((Comparable<?>) doc.getValue(indexQuery.getValue())), entry.getKey());
				return null;
			});
			
			System.out.println(index);
		}
		try {
			Thread.sleep(1000);
		}
		catch (InterruptedException e) {
			return;
		}
	}
	
	@Override
	public void stop() throws Exception {
		stopIndexing = true;
		super.stop();
	}
	
	public void addIndexToCreate(DatabaseCollection collection, String fieldName) {
		indexesQueries.add(new AbstractMap.SimpleEntry<DatabaseCollection, String>(collection, fieldName));
	}
}

