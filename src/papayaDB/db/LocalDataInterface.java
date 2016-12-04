package papayaDB.db;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

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
	private final Map<String, String> users;

	public LocalDataInterface(int listeningPort, HashMap<String, DatabaseCollection> collections) {
		NetServerOptions options = new NetServerOptions().setPort(listeningPort);
		tcpServer = getVertx().createNetServer(options);
		tcpServer.connectHandler(this::onTcpQuery);
		this.collections = collections;
		users = loadAuthorizedUsers();
	}
	
	private HashMap<String, String> loadAuthorizedUsers() {
		HashMap<String, String> users = new HashMap<>();
		Path pathToFile = Paths.get("users");
		
		if (!Files.exists(pathToFile, LinkOption.NOFOLLOW_LINKS)) {
			try {
				Files.createFile(pathToFile);
			}
			catch (IOException e) {
				System.err.println("[DB:LocalInterface:loadAuthorizedUsers]Couldn't open or create the users permission file. Exiting...");
			}
		}
		
		String line;
		try (
		    InputStream fis = new FileInputStream("users");
		    InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
		    BufferedReader br = new BufferedReader(isr);
			) 
		{
		    while ((line = br.readLine()) != null) {
		        String[] infos = line.split(" ");
		        if(infos.length != 2) {
		        	System.err.println("[DB:LocalInterface:loadAuthorizedUsers]Incorrect user information format: "+line);
		        	continue;
		        }
		        users.put(infos[0], infos[1]);
		        System.out.println("[DB:LocalInterface:loadAuthorizedUsers]New authorized user : "+infos[0]+" "+infos[1]);
		    }
		} catch (FileNotFoundException e) {
			System.err.println("[DB:LocalInterface:loadAuthorizedUsers]Couldn't find the users permission file. Exiting...");
		} catch (IOException e) {
			System.err.println("[DB:LocalInterface:loadAuthorizedUsers]Error with the users permission file. Exiting...");
		}
		
		return users;
	}
	
	@Override
	public void start() throws Exception {
		listen();
		super.start();
	}

	public void listen() {
		tcpServer.listen();
		System.out.println("[DB:LocalInterface:listen]Now listening for TCP string queries...");
	}

	@Override
	public void close() {
		tcpServer.close();
		super.close();
	}

	private boolean checkFieldsPresence(JsonObject object, Consumer<QueryAnswer> callback, String... fields) {
		for(String field : fields) {
			if(!object.containsKey(field)) {
				callback.accept(QueryAnswer.buildNewErrorAnswer(QueryAnswerStatus.SYNTAX_ERROR, field+" field is missing"));
				return false;
			}
		}
		return true;
	}
	
	private boolean checkPermission(String user, String hash) {
		return users.get(user).equals(hash);
	}

	public void processQuery(String queryString, Consumer<QueryAnswer> callback) {
		JsonObject query = new JsonObject(queryString);
		QueryType type = null;
		String user = (String) query.getValue("user", null);
		String hash = (String) query.getValue("hash", null);
		
		System.out.println(query);
		try {
			type = QueryType.valueOf(query.getString("type"));
		}
		catch(IllegalArgumentException e) {
			callback.accept(QueryAnswer.buildNewErrorAnswer(QueryAnswerStatus.SYNTAX_ERROR, "type field is missing or field type doesn't exists"));
			return;
		}
		
		if(type == QueryType.CREATEDB) {
			if(checkFieldsPresence(query, callback, "db", "user", "hash")) {
				String dbName = query.getString("db");
				createNewDatabase(dbName, user, hash, callback);
			}
		}
		else if(type == QueryType.DELETEDB) {
			if(checkFieldsPresence(query, callback, "db", "user", "hash")) {
				String dbName = query.getString("db");
				deleteDatabase(dbName, user, hash, callback);
			}
		}
		else if(type == QueryType.EXPORTALL) {
			if(checkFieldsPresence(query, callback, "db")) {
				String dbName = query.getString("db");
				exportDatabase(dbName, callback);
			}
		}
		else if(type == QueryType.GET) {
			if(checkFieldsPresence(query, callback, "db")) {
				String dbName = query.getString("db");
				getRecords(dbName, query.getJsonObject("parameters"), callback);
			}
		}
		else if(type == QueryType.DELETE) {
			if(checkFieldsPresence(query, callback, "db", "user", "hash")) {
				String dbName = query.getString("db");
				deleteRecords(dbName, query.getJsonObject("parameters"), user, hash, callback);
			}
		}
		else if(type == QueryType.INSERT) {
			if(checkFieldsPresence(query, callback, "db", "newRecord", "user", "hash")) {
				String dbName = query.getString("db");
				insertNewRecord(dbName, query.getJsonObject("newRecord"), user, hash, callback);
			}
		}
		else if(type == QueryType.UPDATE) {
			if(checkFieldsPresence(query, callback, "db", "uid", "newRecord", "user", "hash")) {
				String dbName = query.getString("db");
				updateRecord(dbName, query.getString("uid"), query.getJsonObject("newRecord"), user, hash, callback);
			}
		}
	}

	public void onTcpQuery(NetSocket socket) {
		System.out.println("New TCP connection");
		socket.handler(buffer -> {
			String query = buffer.toString();
			System.out.println("Received query: " + buffer.toString());

			processQuery(query, answer -> {
				try {
					socket.write(answer.getData().toString());
					socket.close();
				}
				catch(Exception e) {
					e.printStackTrace();
				}
			});

		});
	}

	
	@Override
	public void createNewDatabase(String name, String user, String hash, Consumer<QueryAnswer> callback) {
		if(!checkPermission(user, hash)) {
			callback.accept(QueryAnswer.buildNewErrorAnswer(QueryAnswerStatus.AUTH_ERROR, "user/hash couple not recognized"));
			return;
		}
		
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
	public void deleteDatabase(String name, String user, String hash, Consumer<QueryAnswer> callback) {
		if(!checkPermission(user, hash)) {
			callback.accept(QueryAnswer.buildNewErrorAnswer(QueryAnswerStatus.AUTH_ERROR, "user/hash couple not recognized"));
			return;
		}
		
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
			List<JsonObject> objects = collection.searchRecords(QueryType.EXPORTALL, null);
			callback.accept(QueryAnswer.buildNewDataAnswer(objects));
		}
	}

	@Override
	public void updateRecord(String database, String uid, JsonObject newRecord, String user, String hash, Consumer<QueryAnswer> callback) {
		if(!checkPermission(user, hash)) {
			callback.accept(QueryAnswer.buildNewErrorAnswer(QueryAnswerStatus.AUTH_ERROR, "user/hash couple not recognized"));
			return;
		}
		
		DatabaseCollection collection = collections.get(database);
		if(collection == null) {
			callback.accept(QueryAnswer.buildNewErrorAnswer(QueryAnswerStatus.STATE_ERROR, "Database "+database+" doesn't exists"));
		}
		else {
			if(collection.updateRecord(uid, newRecord)) {
				callback.accept(QueryAnswer.buildNewEmptyOkAnswer());
			} else {
				callback.accept(QueryAnswer.buildNewErrorAnswer(QueryAnswerStatus.STATE_ERROR, "No object has uid "+uid));
			}
		}
	}

	@Override
	public void deleteRecords(String database, JsonObject parameters, String user, String hash, Consumer<QueryAnswer> callback) {
		if(!checkPermission(user, hash)) {
			callback.accept(QueryAnswer.buildNewErrorAnswer(QueryAnswerStatus.AUTH_ERROR, "user/hash couple not recognized"));
			return;
		}
		
		DatabaseCollection collection = collections.get(database);
		if(collection == null) {
			callback.accept(QueryAnswer.buildNewErrorAnswer(QueryAnswerStatus.STATE_ERROR, "Database "+database+" doesn't exists"));
		}
		else {
			List<JsonObject> objects = collection.searchRecords(QueryType.DELETE, null);
			for(JsonObject object : objects) {
				collection.deleteRecord(object.getString("uid"));
			}
			callback.accept(QueryAnswer.buildNewEmptyOkAnswer());
		}
	}

	@Override
	public void insertNewRecord(String database, JsonObject record, String user, String hash, Consumer<QueryAnswer> callback) {
		if(!checkPermission(user, hash)) {
			callback.accept(QueryAnswer.buildNewErrorAnswer(QueryAnswerStatus.AUTH_ERROR, "user/hash couple not recognized"));
			return;
		}
		
		DatabaseCollection collection = collections.get(database);
		if(collection == null) {
			callback.accept(QueryAnswer.buildNewErrorAnswer(QueryAnswerStatus.STATE_ERROR, "Database "+database+" doesn't exists"));
		}
		else {
			collection.insertNewRecord(record);
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
			List<JsonObject> objects = collection.searchRecords(QueryType.GET, parameters);
			callback.accept(QueryAnswer.buildNewDataAnswer(objects));
		}
	}
}