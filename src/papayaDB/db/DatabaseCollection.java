package papayaDB.db;

import java.io.IOException;
import java.util.ArrayList;
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
	private final FileStorageManager storageFile;
	
	
	public DatabaseCollection(String name) throws IOException {
		this.name = name;
		this.storageFile = new FileStorageManager(name);
	}
	
	private Stream<JsonObject> processParameters(JsonObject query) {
		if(!query.containsKey("type")) throw new SyntaxErrorException("No query type providen");
		String typeString = query.getString("type");
		QueryType type;
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
			
			/*
			for(String parameter : parametersContainer.fieldNames()) {
				JsonObject parameters = parametersContainer.getJsonObject(parameter);
				Optional<QueryParameter> queryParameter = QueryParameter.getQueryParameterKey(QueryType.GET, parameter);
				if(queryParameter.isPresent()) {
					result = queryParameter.get().processQueryParameters(parameters, result);
				}
			}
			*/
		}
		return null;
	}
	
	public ArrayList<JsonObject> searchRecords(QueryType type, JsonObject parameters) {
		System.out.println("Searching records...");
		/*
		Stream<JsonObject> res = elements.values().stream().map(record -> record.getRecord());
		
		res = processParameters(res, query);
		
		return res.collect(Collectors.toList());
		*/
		Stream<JsonObject> res = processParameters(parameters);
		return new ArrayList<>();
	}
}