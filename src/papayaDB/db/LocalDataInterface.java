package papayaDB.db;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;
import java.util.function.Consumer;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import papayaDB.api.query.AbstractChainableQueryInterface;
import papayaDB.api.query.QueryAnswer;
import papayaDB.api.query.QueryAnswerStatus;
import papayaDB.api.query.QueryType;

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
			answer.put("data", new JsonArray(collection.searchRecords(QueryType.GET, jsonQuery)));
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

	@Override
	public void setAuthInformations(String user, String hash) {
		// TODO Auto-generated method stub
	}

	@Override
	public void createNewDatabase(String name, Consumer<QueryAnswer> callback) {
		if(collections.containsKey(name)) {
			callback.accept(QueryAnswer.buildNewErrorAnswer(QueryAnswerStatus.STATE_ERROR, "Database "+name+" already exists"));
		}
		else {
			try {
				collections.put(name, new DatabaseCollection(name));
				callback.accept(QueryAnswer.buildNewEmptyOkAnswer());
			} 
			catch (IOException e) {
				callback.accept(QueryAnswer.buildNewErrorAnswer(QueryAnswerStatus.STATE_ERROR,  "Couldn't create database "+name));
			}
		}
	}

	@Override
	public void deleteDatabase(String name, Consumer<QueryAnswer> callback) {
		if(!collections.containsKey(name)) {
			callback.accept(QueryAnswer.buildNewErrorAnswer(QueryAnswerStatus.STATE_ERROR, "Database "+name+" doesn't exists"));
		}
		else {
			collections.remove(name);
			try {
				Files.delete(Paths.get(name+".coll"));
				callback.accept(QueryAnswer.buildNewEmptyOkAnswer());
			} 
			catch (IOException e) {
				callback.accept(QueryAnswer.buildNewErrorAnswer(QueryAnswerStatus.STATE_ERROR,  "Couldn't delete database "+name));
			}
		}
	}

	@Override
	public void exportDatabase(String database, Consumer<QueryAnswer> callback) {
		DatabaseCollection collection = collections.get(database);
		if(collection == null) {
			callback.accept(QueryAnswer.buildNewErrorAnswer(QueryAnswerStatus.STATE_ERROR, "Database "+database+" doesn't exists"));
		}
		else {
			ArrayList<JsonObject> objects = collection.searchRecords(QueryType.EXPORTALL, null);
			callback.accept(QueryAnswer.buildNewDataAnswer(objects));
		}
	}

	@Override
	public void updateRecord(String database, String uid, JsonObject newRecord, Consumer<QueryAnswer> callback) {
		DatabaseCollection collection = collections.get(database);
		if(collection == null) {
			callback.accept(QueryAnswer.buildNewErrorAnswer(QueryAnswerStatus.STATE_ERROR, "Database "+database+" doesn't exists"));
		}
		else {
			// TODO code update document dans databasecollection
			callback.accept(QueryAnswer.buildNewEmptyOkAnswer());
		}
	}

	@Override
	public void deleteRecords(String database, JsonObject parameters, Consumer<QueryAnswer> callback) {
		DatabaseCollection collection = collections.get(database);
		if(collection == null) {
			callback.accept(QueryAnswer.buildNewErrorAnswer(QueryAnswerStatus.STATE_ERROR, "Database "+database+" doesn't exists"));
		}
		else {
			ArrayList<JsonObject> objects = collection.searchRecords(QueryType.DELETE, null);
			// TODO code delete documents dans databasecollection
			callback.accept(QueryAnswer.buildNewEmptyOkAnswer());
		}
	}

	@Override
	public void insertNewRecord(String database, JsonObject record, Consumer<QueryAnswer> callback) {
		DatabaseCollection collection = collections.get(database);
		if(collection == null) {
			callback.accept(QueryAnswer.buildNewErrorAnswer(QueryAnswerStatus.STATE_ERROR, "Database "+database+" doesn't exists"));
		}
		else {
			// TODO code insert document dans databasecollection	
			callback.accept(QueryAnswer.buildNewEmptyOkAnswer());
		}
	}
	
	@Override
	public void getRecords(String database, JsonObject parameters, Consumer<QueryAnswer> callback) {
		DatabaseCollection collection = collections.get(database);
		if(collection == null) {
			callback.accept(QueryAnswer.buildNewErrorAnswer(QueryAnswerStatus.STATE_ERROR, "Database "+database+" doesn't exists"));
		}
		else {
			ArrayList<JsonObject> objects = collection.searchRecords(QueryType.GET, parameters.getJsonObject("parameters"));
			callback.accept(QueryAnswer.buildNewDataAnswer(objects));
		}
	}
}