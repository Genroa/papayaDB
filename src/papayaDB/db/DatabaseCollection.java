package papayaDB.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.vertx.core.json.JsonObject;
import papayaDB.api.QueryParameters;

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
	private final Map<UUID, Record> elements;
	
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
		this.elements = storageFile.loadRecords();
	}
	
	private Stream<Record> processParameters(Stream<Record> elements, JsonObject query) {
		Stream<Record> result = elements;
		JsonObject parametersContainer = query.getJsonObject("parameters");
		for(String parameter : parametersContainer.fieldNames()) {
			JsonObject parameters = parametersContainer.getJsonObject(parameter);
			result = QueryParameters.valueOf(parameter.toUpperCase()).processQueryParameters(parameters, result);
		}
		return result;
	}
	
	public List<JsonObject> searchRecords(JsonObject query) {
		System.out.println("Searching records...");
		Stream<Record> res = elements.values().stream();
		
		res = processParameters(res, query);
		
		return res.map(record -> record.getRecord()).collect(Collectors.toList());
	}
}