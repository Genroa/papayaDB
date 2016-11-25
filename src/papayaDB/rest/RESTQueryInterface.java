package papayaDB.rest;

import java.util.Optional;
import java.util.function.Consumer;

import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetSocket;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import papayaDB.api.QueryAnswer;
import papayaDB.api.QueryType;
import papayaDB.api.chainable.AbstractChainableQueryInterface;
import papayaDB.api.queryParameters.QueryParameter;

/**
 * Représente une interface web d'accès à une base de données, par l'intermédiaire de routes
 * suivant le REST. 
 *
 */
public class RESTQueryInterface extends AbstractChainableQueryInterface {
	private final HttpServer listeningServer;
	private final NetClient tcpClient;
	private final String host;
	private final int connectionPort;
	private final int listeningPort;
	private final Router router;

	public RESTQueryInterface(String host, int connectionPort, int listeningPort) {
		NetClientOptions options = new NetClientOptions();
		tcpClient = getVertx().createNetClient(options);
		this.host = host;
		this.connectionPort = connectionPort;
		this.listeningPort = listeningPort;

		router = Router.router(getVertx());
		//router.get("/request/*").handler(this::onRESTQuery);		
		router.post("/createdb/:name").handler(x -> this.getRequest(x, QueryType.CREATEDB));
		router.post("/insert/:dbname/:documentname").handler(x -> this.getRequest(x, QueryType.INSERT));
		router.delete("/deletedb/:name").handler(x -> this.getRequest(x, QueryType.DELETEDB));
		router.get("/exportall/:dbname").handler(x -> this.getRequest(x, QueryType.EXPORTALL));
		router.get("/get/*").handler(x -> this.getRequest(x, QueryType.GET));
		router.delete("/deletedocument/:dbname/:params").handler(x -> this.getRequest(x, QueryType.DELETEDOCUMENT));
		
		listeningServer = getVertx().createHttpServer();
	}
	
	@Override
	public void start() throws Exception {
		listen();
		super.start();
	}
	
	public void listen() {
		listeningServer.requestHandler(router::accept).listen(listeningPort);
		System.out.println("Now listening for HTTP REST queries...");
	}
	
	@Override
	public void close() {
		listeningServer.close();
		tcpClient.close();
		super.close();
	}

	@Override
	public void processQuery(String query, Consumer<QueryAnswer> callback) {
		/*
		JsonObject answer = new JsonObject();
		answer.put("status", QueryAnswerStatus.OK.name());
		answer.put("data", new JsonArray().add(query));
		callback.accept(new QueryAnswer(answer));
		*/
		// VRAI CODE EN SUPPOSANT QU'UNE DB EXISTE DE L'AUTRE COTE DE LA CONNEXION
		tcpClient.connect(connectionPort, host, connectHandler -> {
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

	public void onRESTQuery(RoutingContext routingContext) {
		HttpServerResponse response = routingContext.response();
		HttpServerRequest request = routingContext.request();
		String path = request.path();

		System.out.println("Received query " + path);
		
		processQuery(path, answer -> {
			response.putHeader("content-type", "application/json")
			.end(Json.encodePrettily(answer.getData()));
		});
	}
	
	public void createDBRequest(RoutingContext routingContext) {
		//TODO : create method
	}
	
	public void deleteDBRequest(RoutingContext routingContext) {
		System.out.println("Delete DB request");
		//TODO : create method
	}
	
	public void exportAllRequest(RoutingContext routingContext) {
		System.out.println("Export All request");
		//TODO : create method
	}
	
	public void insertRequest(RoutingContext routingContext) {
		System.out.println("Insert request");
		//TODO : create method
	}
	
	public void getRequest(RoutingContext routingContext, QueryType type) {
		HttpServerResponse response = routingContext.response();
		HttpServerRequest request = routingContext.request();
		String path = request.path();
		System.out.println("Received query " + path);
		
		System.out.println("Get request");
		Optional<JsonObject> json = UrlToQuery.convertToJson(routingContext.request().path(), type);
		System.out.println("request " + json.get().toString());
		
		processQuery(json.get().encode(), answer -> {
			response.putHeader("content-type", "application/json")
			.end(Json.encodePrettily(answer.getData()));
		});
	}
	
	public void deleteDocumentRequest(RoutingContext routingContext) {
		System.out.println("Delete Document request");
		//TODO : create method
	}

	
	public static void main(String[] args) {
		RESTQueryInterface RESTInterface = new RESTQueryInterface("localhost", 6666, 8080);
		RESTInterface.listen();
	}
}
