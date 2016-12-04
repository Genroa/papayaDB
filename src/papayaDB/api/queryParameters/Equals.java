package papayaDB.api.queryParameters;

import java.text.NumberFormat;
import java.util.Map;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ser.std.NumberSerializer;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import papayaDB.api.query.QueryType;
import papayaDB.db.FileStorageManager;

public class Equals extends QueryParameter {
	
	public static void registerParameter() {
		QueryParameter.parameter.get(QueryType.GET).put("equals", new Equals());
		QueryParameter.parameter.get(QueryType.UPDATE).put("equals", new Equals());
	}

	public JsonObject valueToJson(JsonObject json, String value) {
		System.out.println("EQUALS VALUE "+value);
		JsonObject params = QueryParameter.getJsonParameters(json);
		JsonArray equals = new JsonArray();
		for (String s: value.split(";")) {
			JsonObject equal = new JsonObject();
			String[] tmp = s.split("=");
			equal.put("field", tmp[0]);
			System.out.println("[EQUALS:ValueParameter]"+tmp[1]);
			try {
				Integer.parseInt(tmp[1]);
				equal.put("value", tmp[1]);
			} catch (NumberFormatException n) {
				equal.put("value", tmp[1].substring(3, tmp[1].length() - 3));
				
			} 
					
			equals.add(equal);
		}
		json.put("parameters", params.put("equals", new JsonObject().put("value", equals)));
		System.out.println("EQUALS JSON "+json);
		return json;
	}
	
	public String valueToString(String key, JsonObject value) {
		StringBuilder sb = new StringBuilder(key).append("/");
		value.getJsonArray("value");
		for (Object ja: value.getJsonArray("value")) { //On itère sur les éléments du json array (retourne un Object)
			JsonObject jo = ((JsonObject)ja);			//On transforme l'Objet en JsonObject
			sb.append(jo.getString("field")).append("=");
			Object valueObject = jo.getValue("value");
			if(valueObject instanceof Integer) {
				sb.append(valueObject.toString()).append(";");
			} else {
				sb.append('"').append(valueObject.toString()).append('"').append(";");
			}
				
		}
		System.out.println("EQUALS: "+sb.deleteCharAt(sb.length() - 1).toString());
		return sb.deleteCharAt(sb.length() - 1).toString();
	}
	
	@Override
	public Stream<Map.Entry<Integer, Integer>> processQueryParameters(JsonObject parameters, Stream<Map.Entry<Integer, Integer>> elements, FileStorageManager storageManager) {
		/*
		 * 1. Récupérer le Stream, fabriquer les JsonObject, comparer sur le champ.
		 * {
			"value":
			[
				{
					"field": fieldName,
					"value": "la value"
				},
				{
					"field": fieldName,
					"value": "la value"
				}
			]
		}
		 */
		JsonArray fieldsParameters = parameters.getJsonArray("value");
		return elements.filter(entry -> {
			JsonObject doc = storageManager.getRecordAtAddress(entry.getKey());
			for(Object paramObject : fieldsParameters) {
				JsonObject param = (JsonObject) paramObject;
				String field = param.getString("field");
				
				System.out.println("Comparaison entre : " + field + " et " + param.getValue("value"));
		
				if(!doc.containsKey(field)) return false;
				if(!doc.getValue(field).equals(param.getValue("value"))) return false;
			}
			return true;
		});
	}
}
 