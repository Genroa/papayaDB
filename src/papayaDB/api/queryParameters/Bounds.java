package papayaDB.api.queryParameters;

import java.util.Map;
import java.util.stream.Stream;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import papayaDB.api.query.QueryType;
import papayaDB.db.DatabaseCollection;

public class Bounds extends QueryParameter {
	public static void registerParameter() {
		QueryParameter.parameter.get(QueryType.GET).put("bounds", new Bounds());
		QueryParameter.parameter.get(QueryType.UPDATE).put("bounds", new Bounds());
	}

	public JsonObject valueToJson(JsonObject json, String value) {
		JsonObject params = QueryParameter.getJsonParameters(json);
		String[] values = value.split(";");
		if(values.length % 3 != 0) {
			//TODO : exception bounds.
		}
		JsonArray bounds = new JsonArray();
		for (int i = 0; i < values.length; i += 3) {
			JsonObject bound = new JsonObject();
			bound.put("field", values[i]);
			try {
				int v1 = Integer.parseInt(values[i+1]);
				int v2 = Integer.parseInt(values[i+2]);
				bound	.put("min", v1)
						.put("max", v2);
			} catch (NumberFormatException n) {
				bound	.put("min", values[i + 1])
						.put("max", values[i + 2]);
			}
			
			bounds.add(bound);
		}
		json.put("parameters", params.put("bounds", new JsonObject().put("value", bounds)));
		return json;
	}
	
	public String valueToString(String key, JsonObject value) {
		StringBuilder sb = new StringBuilder(key).append("/");
		for (Object ja: value.getJsonArray("value")) { //On itère sur les éléments du json array (retourne un Object)
			JsonObject jo = ((JsonObject)ja);			//On transforme l'Objet en JsonObject
			sb	.append(jo.getString("field")).append(";")
				.append(jo.getInteger("min")).append(";")
				.append(jo.getInteger("max")).append(";");
		}
		return sb.deleteCharAt(sb.length() - 1).toString();
	}
	
	@Override
	public Stream<Map.Entry<Integer, Integer>> processQueryParameters(JsonObject parameters, Stream<Map.Entry<Integer, Integer>> elements, DatabaseCollection collection) {
		/*
		 * 1. Récupérer le Stream, fabriquer les JsonObject, comparer sur le champ.
		 * "bounds": 
			{
				"value":
				[
					{
						"field": fieldName,
						"min": 0,
						"max": 12
					},
					{
						"field": fieldName,
						"min": 0,
						"max": 12
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
				
				System.out.println("Comparaison entre : " + field + " et " + param.getValue("min")+" "+param.getValue("max"));
		
				if(!doc.containsKey(field)) return false;
				Object value = doc.getValue(field);
				Object minBound = param.getValue("min");
				Object maxBound = param.getValue("max");
				System.out.println("MAX BOUND CLASS "+maxBound.getClass());
				if(minBound instanceof Number && maxBound instanceof Number && value instanceof Number) {
					if(((double) value) < ((double)minBound) || ((double)value) >= ((double)maxBound)) return false;
				}
				// TODO finir
			}
			return true;
		});
	}
}
 