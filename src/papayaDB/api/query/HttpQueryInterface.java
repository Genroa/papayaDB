package papayaDB.api.query;


import java.util.Objects;
import java.util.function.Consumer;

import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.json.JsonObject;
import papayaDB.api.queryParameters.QueryParameter;

/**
 * Cette classe représente une connexion utilisateur (un "noeud de tête") pour faire des requêtes sur un noeud papayaDB.
 */
class HttpQueryInterface extends AbstractChainableQueryInterface {
	/**
	 * L'objet employé pour le traitement des requêtes HTTP.
	 */
	private final HttpClient client;
	/**
	 * Le port de connexion à l'hôte.
	 */
	private final int port;
	/**
	 * Le nom de l'hôte de la connexion.
	 */
	private final String host;
	
	/**
	 * Crée une nouvelle connexion vers une interface de requête papayaDB.
	 * @param host le nom de l'hôte REST pour la connexion
	 * @param port le port pour la connexion
	 */
	public HttpQueryInterface(String host, int port) {
		client = getVertx().createHttpClient(new HttpClientOptions().setSsl(true).setTrustAll(true));
		this.host = host;
		this.port = port;
	}
	
	@Override
	public void close() {
		client.close();
		super.close();
	}
	
	public void processGetQuery(String query, Consumer<QueryAnswer> callback) {
		Objects.requireNonNull(callback);
		client.getNow(port, host, query, resp -> {
			System.out.println("[HTTPQI : processGetQuery] Got response " + resp.statusCode());
			resp.bodyHandler(body -> {
				callback.accept(new QueryAnswer(body.toJsonObject()));
			});
		});
	}
	
	public void processPostQuery(String query, JsonObject body, String user, String hash, Consumer<QueryAnswer> callback) {
		Objects.requireNonNull(callback);
		query = query + "/auth/[" + user + ";" + hash + "]";
		HttpClientRequest request = client.post(port, host, query, resp -> {
			System.out.println("[HTTPQI : processPostQuery] Got response " + resp.statusCode());
			resp.bodyHandler(bodyResponse -> {
				callback.accept(new QueryAnswer(bodyResponse.toJsonObject()));
			});
		});
		if(body == null) {
			request.end();
		} else {
			request.end(body.toString());
		}
		
	}
	
	public void processDeleteQuery(String query, String user, String hash, Consumer<QueryAnswer> callback) {
		Objects.requireNonNull(callback);
		query = query + "/auth/[" + user + ";" + hash + "]";
		client.delete(port, host, query, resp -> {
			System.out.println("[HTTPQI : processDeleteQuery] Got response " + resp.statusCode());
			resp.bodyHandler(bodyResponse -> {
				callback.accept(new QueryAnswer(bodyResponse.toJsonObject()));
			});
		}).end();
	}

	@Override
	public void createNewDatabase(String name, String user, String hash, Consumer<QueryAnswer> callback) {
		Objects.requireNonNull(name);
		processPostQuery("/createdb/" + name, null, user, hash, callback);
	}

	@Override
	public void deleteDatabase(String name, String user, String hash, Consumer<QueryAnswer> callback) {
		Objects.requireNonNull(name);
		processDeleteQuery("/deletedb/" + name, user, hash, callback);
	}

	@Override
	public void exportDatabase(String database, Consumer<QueryAnswer> callback) {
		Objects.requireNonNull(database);
		processGetQuery("/exportall/" + database ,callback);
	}

	@Override
	public void updateRecord(String database, String uid, JsonObject newRecord, String user, String hash, Consumer<QueryAnswer> callback) {
		Objects.requireNonNull(database);
		Objects.requireNonNull(uid);
		Objects.requireNonNull(newRecord);
		processPostQuery("/update/" + database, new JsonObject().put("uid", uid).put("record", newRecord), user, hash, callback);
	}

	@Override
	public void deleteRecords(String database, JsonObject parameters, String user, String hash, Consumer<QueryAnswer> callback) {
		Objects.requireNonNull(database);
		StringBuilder sb = new StringBuilder("/delete/" + database);
		
		if(parameters != null) {
			for (String key: parameters.fieldNames()) {
				// La méthode getQueryParameterKey sert à récuperer l'instance de la clé actuelle.
				// valueToString permet de convertir l'objet json en une chaine utilisable dans l'URL
				sb.append("/" + QueryParameter.getQueryParameterKey(QueryType.GET, key).get().valueToString(key, parameters.getJsonObject(key)));
			}
		}
		processDeleteQuery(sb.toString(), user, hash, callback);
	}

	@Override
	public void insertNewRecord(String database, JsonObject record, String user, String hash, Consumer<QueryAnswer> callback) {
		Objects.requireNonNull(database);
		Objects.requireNonNull(record);
		processPostQuery("/insert/" + database, record, user, hash, callback);
	}
	
	@Override
	public void getRecords(String database, JsonObject parameters, Consumer<QueryAnswer> callback) {
		Objects.requireNonNull(database);
		StringBuilder sb = new StringBuilder("/get/" + database);
		
		if(parameters != null) {
			for (String key: parameters.fieldNames()) {
				// La méthode getQueryParameterKey sert à récuperer l'instance de la clé actuelle.
				// valueToString permet de convertir l'objet json en une chaine utilisable dans l'URL
				sb.append("/" + QueryParameter.getQueryParameterKey(QueryType.GET, key).get().valueToString(key, parameters.getJsonObject(key)));
			}
		}
		processGetQuery(sb.toString(), callback);
	}
}