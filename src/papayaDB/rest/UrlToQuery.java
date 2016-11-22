package papayaDB.rest;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;

import io.vertx.core.json.JsonObject;
import papayaDB.api.QueryParameters;

public class UrlToQuery {
	private static final String REGEX = "\\/(?=(?:[^%22]*%22[^%22]*%22)*[^%22]*$)";
	private final static Map<String, BiConsumer<JsonObject,String>> map = new HashMap<>();
	static {
		map.put("get", (JsonObject json, String value) -> QueryParameters.TYPE.valueToJson(json, value));
		map.put("db", (JsonObject json, String str) -> QueryParameters.DB.valueToJson(json, str));
		map.put("fields", (JsonObject json, String str) -> QueryParameters.FIELDS.valueToJson(json, str));
		map.put("bounds", (JsonObject json, String str) -> QueryParameters.BOUNDS.valueToJson(json, str));
		map.put("limit", (JsonObject json, String str) -> QueryParameters.LIMIT.valueToJson(json, str));
		map.put("equals", (JsonObject json, String str) -> QueryParameters.EQUALS.valueToJson(json, str));
		map.put("order", (JsonObject json, String str) -> QueryParameters.ORDER.valueToJson(json, str));
	}
	
	public static Optional<JsonObject> convertToJson(String url) {
		JsonObject json = new JsonObject();
		Objects.requireNonNull(url);
		String[] params = url.substring(1).split(REGEX); //remove the first '/' and split at all others except if between " ".
		if(params.length % 2 != 0) {
			return Optional.empty();
		}
		put(json, params[0], params[0]);
		put(json, "db", params[1]);
		for (int i = 2; i < params.length; i += 2) {
			System.out.println(params[i] + " " + params[i+1]);
			put(json, params[i], params[i+1]);
		}
		return Optional.of(json);
	}

	private static void put(JsonObject json, String key, String value) {
		map.get(key).accept(json, value);
	}
}
