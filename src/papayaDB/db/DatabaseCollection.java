package papayaDB.db;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import io.vertx.core.json.JsonObject;

/**
 * Classe représentant une Collection de base de données.
 */
public class DatabaseCollection {
	/**
	 * Le nom de la DatabaseCollection
	 */
	private final String name;
	
	/**
	 * La Map contenant les éléments de la DatabaseCollection
	 */
	private final Map<UUID, FileRecord> elements = new HashMap<>();
	
	/**
	 * La Map contenant les TreeMap représentant les index d'optimisation sur la Map des éléments
	 */
	private final Map<String, TreeMap<?, UUID>> indexes = new HashMap<>();
	
	/**
	 * Le StorageManager stockant physiquement la collection
	 */
	private final StorageManager storageFile;
	
	
	public DatabaseCollection(String name) throws IOException {
		this.name = name;
		this.storageFile = new FileStorageManager(name);		
	}
}