package papayaDB.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import io.vertx.core.AbstractVerticle;

public class DatabaseCleaningManager extends AbstractVerticle {
	private final HashMap<String, DatabaseCollection> collections;
	private boolean stopCleaning = false;
	
	public DatabaseCleaningManager(HashMap<String, DatabaseCollection> collections) {
		this.collections = collections;
	}
	
	@Override
	public void start() {
		clean();
	}
	
	public void clean() {
		while(!stopCleaning) {
			for(DatabaseCollection collection : collections.values()) {
				FileStorageManager storageManager = collection.getStorageManager();
				HashMap<Integer, ArrayList<Integer>> holesMap = storageManager.getHolesMap();
				HashMap<Integer, Integer> addressMap = storageManager.getRecordsMap();
				
				// Trouver un trou
				for(Entry<Integer, ArrayList<Integer>> holesEntry : holesMap.entrySet()) {
					int chunkSize = holesEntry.getKey();
					ArrayList<Integer> holes = holesEntry.getValue();
					if(holes.size() == 0) {
						continue;
					}
					
					int holeAddress = holes.get(0);
					
					// Trouver si un objet se trouve derrière
					int supposedRecordAddress = holeAddress + chunkSize * FileStorageManager.CHUNK_SIZE;
					if(addressMap.containsKey(supposedRecordAddress)) {
						int size = addressMap.get(supposedRecordAddress);
						
						System.out.println(holesMap);
						System.out.println(addressMap);
						storageManager.copyRecordToNewAddressWithLength(supposedRecordAddress, holeAddress, FileStorageManager.computeChunkSize(size)/FileStorageManager.CHUNK_SIZE, size);
						
						System.out.println("[DB:CleaningThread:cleaning] Cleaned a hole in collection "+collection+", "+supposedRecordAddress +" => "+holeAddress);
						System.out.println(holesMap);
						System.out.println(addressMap);
						
						break; // On passe à une autre collection
					}
				}
			}
			try {
				Thread.sleep(1000);
			}
			catch (InterruptedException e) {
				return;
			}
		}
	}
	
	@Override
	public void stop() throws Exception {
		stopCleaning = true;
		super.stop();
	}
}
