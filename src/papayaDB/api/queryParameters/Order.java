package papayaDB.api.queryParameters;

import java.util.Map;
import java.util.stream.Stream;

import io.vertx.core.json.JsonObject;
import papayaDB.api.query.QueryType;
import papayaDB.api.query.SyntaxErrorException;
import papayaDB.db.FileStorageManager;

public class Order extends QueryParameter {
	public static void registerParameter() {
		QueryParameter.parameter.get(QueryType.GET).put("order", new Order());
	}

	public JsonObject valueToJson(JsonObject json, String value) {
		JsonObject params = QueryParameter.getJsonParameters(json);
		String[] values = value.split(";");
		if(values.length != 2) {
			throw new SyntaxErrorException("You must specify a field and an order (ASC or DESC)");
		}
		json.put("parameters", params.put("order", new JsonObject().put("field", values[0]).put("way", values[1])));
		return json;
	}
	
	public String valueToString(String key, JsonObject value) {
		StringBuilder sb = new StringBuilder(key).append("/");
		sb.append(value.getString("field")).append(";").append(value.getString("way"));
		return sb.toString();
	}
	
	@Override
	public Stream<Map.Entry<Integer, Integer>> processQueryParameters(JsonObject parameters, Stream<Map.Entry<Integer, Integer>> elements, FileStorageManager storageManager) {
		/*
		 * 1. Récupérer le Stream, fabriquer les JsonObject, comparer sur les champs, 
		 */
		// TODO faire le order
		return null;
	}
}
 