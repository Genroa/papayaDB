package papayaDB.api;

import java.util.stream.Stream;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import papayaDB.db.Record;

public enum QueryParameters {
	
	TYPE {
		@Override
		public JsonObject valueToJson(JsonObject json, String value) {
			json.put("type", value);
			return json;
		}
	},
	
	DB {
		@Override
		public JsonObject valueToJson(JsonObject json, String value) {
			json.put("db", value);
			return json;
		} 
	},
	
	FIELDS {
		@Override
		public JsonObject valueToJson(JsonObject json, String value) {
			JsonObject params = getParams(json);
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
	},
	
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
	
	LIMIT {
		@Override
		public JsonObject valueToJson(JsonObject json, String value) {
			JsonObject params = getParams(json);
			json.put("parameters", params.put("limit", new JsonObject().put("value", Integer.parseInt(value))));
			return json;
		}

		@Override
		public Stream<Record> processQueryParameters(JsonObject parameters, Stream<Record> elements) {
			long maxSize = parameters.getLong("value");
			return elements.limit(maxSize);
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
