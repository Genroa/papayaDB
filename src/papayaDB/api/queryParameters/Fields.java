package papayaDB.api.queryParameters;

import java.util.stream.Stream;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import papayaDB.api.query.QueryType;
import papayaDB.api.query.SyntaxErrorException;
import papayaDB.db.FileStorageManager;

public class Fields extends QueryParameter {
	public static void registerParameter() {
		QueryParameter.parameter.get(QueryType.DELETE).put("fields", new Fields());
		QueryParameter.parameter.get(QueryType.GET).put("fields", new Fields());
		QueryParameter.parameter.get(QueryType.INSERT).put("fields", new Fields());
		QueryParameter.parameter.get(QueryType.UPDATE).put("fields", new Fields());
	}
	
	public Fields() {
		isTerminalModifier = true;
	}

	public JsonObject valueToJson(JsonObject json, String value) {
		JsonObject params = QueryParameter.getJsonParameters(json);
		JsonObject fields = new JsonObject();
		JsonArray array = new JsonArray();
		
		String[] values = value.split(","); //On retire les crochets et on split aux virgules
		for	(String s: values) { // on ajoute chaque élément au tableau json
			array.add(s);
		}
		fields.put("value", array);
		json.put("parameters", params.put("fields", fields));
		return json;
	}
	
	// L'ordre d'appel des queryParameters compte!
	public Stream<JsonObject> processTerminalOperation(JsonObject parameters, Stream<JsonObject> elements, FileStorageManager storageManager) {
		JsonArray array = parameters.getJsonArray("value");
		
		return elements.filter(json -> {
			for(Object field : array) {
				if(! (field instanceof String)) {
					// TODO l'exception ne sert à rien ici (lambda), revoir ce code
					throw new SyntaxErrorException("Field "+field+" must be a field name, represented by a string");
				}
				if(!json.containsKey((String) field)) return false;
			}
			return true;
		}).map(oldJson -> {
			JsonObject newJson = new JsonObject();
			for(Object field : array) {
				newJson.put((String)field, oldJson.getValue((String) field));
			}
			return newJson;
		});
	}
	
	public String valueToString(String key, JsonObject value) {
		StringBuilder sb = new StringBuilder(key + "/" );
		if(value.getJsonObject("value").toString().startsWith("[")) {
			sb.append(value.getJsonArray("value").toString());
		} else {
			sb.append(value.getValue("value").toString()).append(",");
		}
		return sb.toString();
	}
}
 