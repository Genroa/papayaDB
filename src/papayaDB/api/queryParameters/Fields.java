package papayaDB.api.queryParameters;

import java.util.stream.Stream;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import papayaDB.api.QueryType;
import papayaDB.api.SyntaxErrorException;

public class Fields extends QueryParameter {
	public static void registerParameter() {
		QueryParameter.parameter.get(QueryType.DELETEFIELD).put("fields", new Fields());
		QueryParameter.parameter.get(QueryType.GET).put("fields", new Fields());
		QueryParameter.parameter.get(QueryType.INSERT).put("fields", new Fields());
		QueryParameter.parameter.get(QueryType.UPDATE).put("fields", new Fields());
	}

	public JsonObject valueToJson(JsonObject json, String value) {
		JsonObject params = QueryParameter.getJsonParameters(json);
		JsonArray array = new JsonArray();
		if(value.charAt(0) == '[' && value.charAt(value.length()-1) == ']') {
			value = value.substring(1, value.length() - 1);
			String[] values = value.split(",");
			for	(String s: values) {
				array.add(s);
			}
		} else {
			array.add(value);
		}
		json.put("parameters", params.put("fields", new JsonObject().put("value", array)));
		return json;
	}
	
	// L'ordre d'appel des queryParameters compte!
	public Stream<JsonObject> processQueryParameters(JsonObject parameters, Stream<JsonObject> elements) {
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
}
 