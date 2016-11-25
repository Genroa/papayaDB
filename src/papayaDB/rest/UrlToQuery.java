package papayaDB.rest;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import io.vertx.core.json.JsonObject;
import papayaDB.api.QueryType;
import papayaDB.api.queryParameters.QueryParameter;

public class UrlToQuery {
	private static final String REGEX = "\\/(?=(?:[^%22]*%22[^%22]*%22)*[^%22]*$)";
	
	
	public static Optional<JsonObject> convertToJson(String url, QueryType type) {
		JsonObject json = new JsonObject();
		Objects.requireNonNull(url);
		String[] params = url.substring(1).split(REGEX); //remove the first '/' and split at all others except if between " ".
		if(params.length % 2 != 0) {
			return Optional.empty();
		}
		put(json, "type", type.toString(), type);
		put(json, "db", params[1], type);
		for (int i = 2; i < params.length; i += 2) {
			System.out.println(params[i] + " " + params[i+1]);
			put(json, params[i], params[i+1], type);
		}
		return Optional.of(json);
	}

	private static void put(JsonObject json, String key, String value, QueryType type) {
		System.out.println("TRY PUT : " + key + " with value " + value);
		Optional<QueryParameter> query = QueryParameter.getQueryParameterKey(type, key);
		if(!query.isPresent()) {
			json = new JsonObject();
			return;
		}
		query.get().valueToJson(json, value);
	}
}
