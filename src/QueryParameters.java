

import java.util.stream.Stream;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import papayaDB.db.Record;

public enum QueryParameters {	
	BOUNDS {
		@Override
		public JsonObject valueToJson(JsonObject json, String value) {
			JsonObject params = getParams(json);
			value = value.substring(1, value.length() - 1);
			String[] values = value.split(";");
			if(values.length % 3 != 0) {
				//TODO : exception bounds.
			}
			JsonArray bounds = new JsonArray();
			for (int i = 0; i < values.length; i += 3) {
				JsonObject bound = new JsonObject();
				bound	.put("field", values[i])
						.put("min", values[i + 1])
						.put("max", values[i + 2]);
				bounds.add(bound);
			}
			json.put("parameters", params.put("bounds", new JsonObject().put("value", bounds)));
			return json;
		}
	},
	
	EQUALS {
		@Override
		public JsonObject valueToJson(JsonObject json, String value) {
			JsonObject params = getParams(json);
			value = value.substring(1, value.length() - 1);
			String[] values = value.split(";");
			JsonArray equals = new JsonArray();
			for (String s: value.split(";")) {
				JsonObject equal = new JsonObject();
				String[] tmp = s.split("=");
				equal	.put("field", tmp[0])
						.put("value", tmp[1].substring(3, tmp[1].length() - 3));
				equals.add(equal);
			}
			json.put("parameters", params.put("equals", new JsonObject().put("value", equals)));
			return json;
		}
	},
	
	ORDER {
		@Override
		public JsonObject valueToJson(JsonObject json, String value) {
			JsonObject params = getParams(json);
			value = value.substring(1, value.length() - 1);
			String[] values = value.split(";");
			if(values.length != 2) {
				//TODO: exception order;
			}
			json.put("parameters", params.put("order", new JsonObject().put("field", values[0]).put("way", values[1])));
			return json;
		}
	};
	
	
	public abstract JsonObject valueToJson(JsonObject json, String value);
	
	public Stream<Record> processQueryParameters(JsonObject parameters, Stream<Record> elements) {
		return elements;
	};
	
	private static JsonObject getParams(JsonObject json) {
		if(json.containsKey("parameters")) {
			return json.getJsonObject("parameters");
		}
		return new JsonObject();
	}
}
