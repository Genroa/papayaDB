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
		router.post("/createdb/*").handler(x -> this.onRESTQuery(x, QueryType.CREATEDB));
		router.post("/insert/*").handler(x -> this.onRESTQuery(x, QueryType.INSERT));
		router.delete("/deletedb/*").handler(x -> this.onRESTQuery(x, QueryType.DELETEDB));
		router.get("/exportall/*").handler(x -> this.onRESTQuery(x, QueryType.EXPORTALL));
		router.get("/get/*").handler(x -> this.onRESTQuery(x, QueryType.GET));
		router.delete("/deletedocument/*").handler(x -> this.onRESTQuery(x, QueryType.DELETEDOCUMENT));
		
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
	
	public void onRESTQuery(RoutingContext routingContext, QueryType type) {
		HttpServerResponse response = routingContext.response();
		HttpServerRequest request = routingContext.request();
		String path = request.path();
		System.out.println("Received query " + path);
		
		System.out.println("Get request");
		JsonObject json = UrlToQuery.convertToJson(routingContext.request().path(), type);
		if(json.containsKey("status")) {
			response.putHeader("content-type", "application/json")
			.end(Json.encodePrettily(json));
			return;
		}
		
		processQuery(json.encode(), answer -> {
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
