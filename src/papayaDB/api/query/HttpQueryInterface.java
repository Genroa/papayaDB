package papayaDB.api.query;


import java.util.Objects;
import java.util.function.Consumer;

import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;

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
	
	private final String user;
	
	private final String hash;
	
	/**
	 * Crée une nouvelle connexion vers une interface de requête papayaDB.
	 * @param host le nom de l'hôte REST pour la connexion
	 * @param port le port pour la connexion
	 */
	public HttpQueryInterface(String host, int port, String user, String hash) {
		client = getVertx().createHttpClient();
		this.host = host;
		this.port = port;
		this.user = user;
		this.hash = hash;
	}
	
	@Override
	public void close() {
		client.close();
		super.close();
	}
	
	@Override
	public void processQuery(String query, Consumer<QueryAnswer> callback) {
		Objects.requireNonNull(callback);
		client.getNow(port, host, "/"+query, response -> {
			//TODO check response.statusCode() before calling bodyHandler
			response.bodyHandler(bodyBuffer -> { callback.accept(new QueryAnswer(bodyBuffer.toJsonObject())); });
		});
	}

	@Override
	public void createNewDatabase(String name, Consumer<QueryAnswer> callback) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteDatabase(String name, Consumer<QueryAnswer> callback) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateRecord(String uid, JsonObject newRecord, Consumer<QueryAnswer> callback) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteRecords(String uid, Consumer<QueryAnswer> callback) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void insertNewRecord(String database, JsonObject record, Consumer<QueryAnswer> callback) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void getRecords(JsonObject query, Consumer<QueryAnswer> callback) {
		// TODO Auto-generated method stub
		
	}
}