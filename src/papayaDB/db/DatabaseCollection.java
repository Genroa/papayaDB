package papayaDB.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.vertx.core.json.JsonObject;
import papayaDB.api.query.QueryType;
import papayaDB.api.queryParameters.QueryParameter;

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
	
	private Stream<JsonObject> processParameters(Stream<JsonObject> elements, JsonObject query) {
		Stream<JsonObject> result = elements;
		JsonObject parametersContainer = query.getJsonObject("parameters");
		if(parametersContainer != null) {
			for(String parameter : parametersContainer.fieldNames()) {
				JsonObject parameters = parametersContainer.getJsonObject(parameter);
				Optional<QueryParameter> queryParameter = QueryParameter.getQueryParameterKey(QueryType.GET, parameter);
				if(queryParameter.isPresent()) {
					result = queryParameter.get().processQueryParameters(parameters, result);
				}
			}
		}
		return result;
	}
	
	public List<JsonObject> searchRecords(JsonObject query) {
		System.out.println("Searching records...");
		Stream<JsonObject> res = elements.values().stream().map(record -> record.getRecord());
		
		res = processParameters(res, query);
		
		return res.collect(Collectors.toList());
	}
}