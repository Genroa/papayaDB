package papayaDB.db;

import java.util.Map;
import java.util.function.Consumer;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import papayaDB.api.QueryAnswer;
import papayaDB.api.QueryAnswerStatus;
import papayaDB.api.chainable.AbstractChainableQueryInterface;

public class LocalDataInterface extends AbstractChainableQueryInterface {
	private final NetServer tcpServer;
	private final Map<String, DatabaseCollection> collections;
	
	public LocalDataInterface(int listeningPort, Map<String, DatabaseCollection> collections) {
		NetServerOptions options = new NetServerOptions().setPort(listeningPort);
		tcpServer = getVertx().createNetServer(options);
		tcpServer.connectHandler(this::onTcpQuery);
		this.collections = collections;
	}
	
	@Override
	public void start() throws Exception {
		listen();
		super.start();
	}

	public void listen() {
		tcpServer.listen();
		System.out.println("Now listening for TCP string queries...");
	}

	@Override
	public void close() {
		tcpServer.close();
		super.close();
	}

	@Override
	public void processQuery(String query, Consumer<QueryAnswer> callback) {
		JsonObject answer = new JsonObject();
		System.out.println(query);
		JsonObject jsonQuery = new JsonObject(query);
		// TODO vérification à faire sur la clef
		String dbName = jsonQuery.getString("db");
		DatabaseCollection collection = collections.get(dbName);
		
		// Erreur, db innexistante
		if(collection == null) {
			answer.put("status", QueryAnswerStatus.SYNTAX_ERROR.name());
			answer.put("message", "db "+dbName+" doesn't exist");
			callback.accept(new QueryAnswer(answer));
			return;
		}
		
		if(jsonQuery.getString("type").equals("GET")) {
			System.out.println("Get request, let's launch searchRecords...");
			answer.put("status", QueryAnswerStatus.OK.name());
			answer.put("data", new JsonArray(collection.searchRecords(jsonQuery)));
		} else { /* type inconnu */
			answer.put("status", QueryAnswerStatus.OK.name());
			answer.put("data", new JsonArray());
		}
		

		callback.accept(new QueryAnswer(answer));
	}

	public void onTcpQuery(NetSocket socket) {
		System.out.println("New connection!");
		socket.handler(buffer -> {
			String query = buffer.toString();
			System.out.println("Received query: " + buffer.toString());

			processQuery(query, answer -> {
				socket.write(answer.getData().toString());
				socket.close();
			});

		});
	}
}