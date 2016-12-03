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
	
	private final String user;
	
	private final String hash;
	
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
	
	@Override
	public void processQuery(String query, Consumer<QueryAnswer> callback) {
		Objects.requireNonNull(callback);
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
				socket.write(query);

			} else {
				System.out.println("Failed to connect: " + connectHandler.cause().getMessage());
			}
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