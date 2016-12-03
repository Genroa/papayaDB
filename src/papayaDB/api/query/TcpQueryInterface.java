package papayaDB.api.query;


import java.util.Objects;
import java.util.function.Consumer;

import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetSocket;

/**
 * Cette classe représente une connexion utilisateur (un "noeud de tête") pour faire des requêtes sur un noeud papayaDB.
 */
class TcpQueryInterface extends AbstractChainableQueryInterface {
	/**
	 * L'objet employé pour le traitement des requêtes.
	 */
	private final NetClient client;
	/**
	 * Le port de connexion à l'hôte.
	 */
	private final int port;
	
	private String host;
	
	private String user;
	
	private String hash;
	
	/**
	 * Crée une nouvelle connexion vers une interface de requête papayaDB.
	 * @param host le nom de l'hôte REST pour la connexion
	 * @param port le port pour la connexion
	 */
	public TcpQueryInterface(String host, int port, String user, String hash) {
		NetClientOptions options = new NetClientOptions();
		client = getVertx().createNetClient(options);
		this.port = port;
		this.host = host;
		this.user = user;
		this.hash = hash;
	}
	
	
	@Override
	public void close() {
		client.close();
		super.close();
	}
	
	public void processQuery(String database, JsonObject queryObject, Consumer<QueryAnswer> callback) {
		Objects.requireNonNull(queryObject);
		Objects.requireNonNull(callback);
		
		queryObject.put("db", database)
				   .put("user", user)
				   .put("password", hash);
		
		
		client.connect(port, host, connectHandler -> {
			if (connectHandler.succeeded()) {
				System.out.println("Connection established for query");
				NetSocket socket = connectHandler.result();
				
				// Définir quoi faire avec la réponse
				socket.handler(buffer -> {
					JsonObject answer = buffer.toJsonObject();
					System.out.println("Received query answer: "+answer);
					callback.accept(new QueryAnswer(answer));
				});
				
				// Envoyer la demande
				socket.write(queryObject.encode());

			} else {
				callback.accept(new QueryAnswer(new JsonObject().put("status", QueryAnswerStatus.HOST_UNREACHABLE.name()).put("message", "Query couldn't reach next host")));
			}
		});
	}
	

	@Override
	public void createNewDatabase(String name, Consumer<QueryAnswer> callback) {
		Objects.requireNonNull(name);
		processQuery(name, new JsonObject().put("type", QueryType.CREATEDB.name()), callback);
	}


	@Override
	public void deleteDatabase(String name, Consumer<QueryAnswer> callback) {
		Objects.requireNonNull(name);
		processQuery(name, new JsonObject().put("type", QueryType.DELETEDB.name()), callback);
	}


	@Override
	public void updateRecord(String database, String uid, JsonObject newRecord, Consumer<QueryAnswer> callback) {
		Objects.requireNonNull(uid);
		Objects.requireNonNull(newRecord);
		processQuery(database, new JsonObject().put("type", QueryType.UPDATE.name())
									 			.put("oldRecord", uid)
									 			.put("newRecord", newRecord), callback);
	}
	

	@Override
	public void deleteRecords(String database, JsonObject parameters, Consumer<QueryAnswer> callback) {
		processQuery(database, new JsonObject().put("type", QueryType.DELETE).put("parameters", parameters), callback);
	}
	

	@Override
	public void insertNewRecord(String database, JsonObject record, Consumer<QueryAnswer> callback) {
		Objects.requireNonNull(database);
		Objects.requireNonNull(record);
		processQuery(database, new JsonObject().put("type", QueryType.INSERT.name()).put("newRecord", record), callback);
	}


	@Override
	public void getRecords(String database, JsonObject parameters, Consumer<QueryAnswer> callback) {
		processQuery(database, new JsonObject().put("type", QueryType.GET).put("parameters", parameters), callback);
	}


	@Override
	public void setAuthInformations(String user, String hash) {
		this.user = user;
		this.hash = hash;
	}


	@Override
	public void exportDatabase(String database, Consumer<QueryAnswer> callback) {
		processQuery(database, new JsonObject().put("type", QueryType.EXPORTALL), callback);
	}
}