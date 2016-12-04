package papayaDB.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.vertx.core.json.JsonObject;
import papayaDB.api.query.QueryType;
import papayaDB.api.query.SyntaxErrorException;
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
	 * La Map contenant les TreeMap représentant les index d'optimisation sur la Map des éléments
	 */
	private final Map<String, TreeMap<?, Integer>> indexes = new HashMap<>();
	
	/**
	 * Le StorageManager stockant physiquement la collection
	 */
	private final FileStorageManager storageManager;
	
	
	public DatabaseCollection(String name) throws IOException {
		this.name = name;
		this.storageManager = new FileStorageManager(name);
	}
	
	private Stream<JsonObject> processParameters(JsonObject query) {
		if(!query.containsKey("type")) throw new SyntaxErrorException("No query type providen");
		String typeString = query.getString("type");
		QueryType type;
		Stream<JsonObject> terminalResult = null;
		Stream<Entry<Integer, Integer>> result = storageManager.getRecordsMap().entrySet().stream();
		
		try {
			type = QueryType.valueOf(typeString);
		}
		catch(IllegalArgumentException e) {
			throw new SyntaxErrorException("Query type "+typeString+" doesn't exists");
		}
		
		JsonObject parametersContainer = query.getJsonObject("parameters");
		if(parametersContainer != null) {
			ArrayList<String> parametersNames = new ArrayList<>(parametersContainer.fieldNames());
			parametersNames.sort((parameterName1, parameterName2) -> {
				Optional<QueryParameter> qp1 = QueryParameter.getQueryParameterKey(type, parameterName1);
				Optional<QueryParameter> qp2 = QueryParameter.getQueryParameterKey(type, parameterName2);
				
				if(!qp1.isPresent()) throw new SyntaxErrorException("Query parameter "+parameterName1+" doesn't exists or isn't correct with query type "+type.name());
				if(!qp2.isPresent()) throw new SyntaxErrorException("Query parameter "+parameterName2+" doesn't exists or isn't correct with query type "+type.name());
				
				QueryParameter q1 = qp1.get();
				QueryParameter q2 = qp2.get();
				
				if(!q1.isTerminalModifier()) return -1;
				if(!q2.isTerminalModifier()) return 1;
				
				return 0;
			});
			
			boolean reachedTerminalOperations = false;
			for(String parameter : parametersContainer.fieldNames()) {
				JsonObject parameters = parametersContainer.getJsonObject(parameter);
				Optional<QueryParameter> queryParameter = QueryParameter.getQueryParameterKey(QueryType.GET, parameter);
				if(queryParameter.isPresent()) {
					QueryParameter qp = queryParameter.get();
					
					// Si on a atteint les modificateurs terminaux
					if(qp.isTerminalModifier()) {
						if(!reachedTerminalOperations) {
							reachedTerminalOperations = true;
							terminalResult = convertAddressStreamToTerminal(result);
						}
						terminalResult = qp.processTerminalOperation(parameters, terminalResult, storageManager);
					}
					else {
						result = qp.processQueryParameters(parameters, result, storageManager);
					}
				}
			}
		}
		
		if(terminalResult == null) {
			terminalResult = convertAddressStreamToTerminal(result);
		}
		
		return terminalResult;
	}
	
	private Stream<JsonObject> convertAddressStreamToTerminal(Stream<Entry<Integer, Integer>> elements) {
		if(elements == null) return Stream.empty();
		
		return elements.map(entry -> {
			return storageManager.getRecordAtAddress(entry.getKey());
		});
	}
	
	public List<JsonObject> searchRecords(QueryType type, JsonObject parameters) {
		System.out.println("Searching records...");
		Stream<JsonObject> res = processParameters(parameters);
		return res.collect(Collectors.toList());
	}
	
	
	public boolean updateRecord(String uid, JsonObject newRecord) {
		// TODO UTILISER UN INDEX SUR UID
		Optional<Integer> optionalAddress = storageManager.getRecordsMap().keySet().stream().filter(key -> {
			JsonObject doc = storageManager.getRecordAtAddress(key);
			return doc.getString("uid").equals(uid);
		}).findFirst();
		
		if(optionalAddress.isPresent()) {
			storageManager.updateRecord(optionalAddress.get(), newRecord);
			return true;
		}
		return false;
	}
	
	public void deleteRecord(String uid) {
		// TODO UTILISER UN INDEX SUR UID
		Optional<Integer> optionalAddress = storageManager.getRecordsMap().keySet().stream().filter(key -> {
			JsonObject doc = storageManager.getRecordAtAddress(key);
			return doc.getString("uid").equals(uid);
		}).findFirst();
		
		if(optionalAddress.isPresent()) {
			storageManager.deleteRecordAtAddress(optionalAddress.get());
		}
	}
	
	
	public void insertNewRecord(JsonObject record) {
		storageManager.createNewRecord(record);
	}
}