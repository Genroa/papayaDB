package papayaDB.rest;

import java.util.function.Consumer;

import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import papayaDB.api.query.AbstractChainableQueryInterface;
import papayaDB.api.query.QueryAnswer;
import papayaDB.api.query.QueryInterface;
import papayaDB.api.query.QueryType;

/**
 * Représente une interface web d'accès à une base de données, par l'intermédiaire de routes
 * suivant le REST. 
 *
 */
public class RESTQueryInterface extends AbstractChainableQueryInterface {
	private final HttpServer listeningServer;
	private final QueryInterface tcpClient;
	private final int listeningPort;
	private final Router router;

	public RESTQueryInterface(String host, int connectionPort, int listeningPort) {
		
		HttpServerOptions options = new HttpServerOptions().setSsl(true).setKeyStoreOptions(
				new JksOptions()
					.setPath("keystore.jks")
					.setPassword("papayadb")
				);
		
		tcpClient = QueryInterface.newTcpQueryInterface(host, connectionPort);
		this.listeningPort = listeningPort;

		router = Router.router(getVertx());
		router.post("/createdb/*").handler(x -> this.onRESTQuery(x, QueryType.CREATEDB));
		router.post("/insert/*").handler(x -> this.onRESTQuery(x, QueryType.INSERT));
		router.delete("/deletedb/*").handler(x -> this.onRESTQuery(x, QueryType.DELETEDB));
		router.get("/exportall/*").handler(x -> this.onRESTQuery(x, QueryType.EXPORTALL));
		router.get("/get/*").handler(x -> this.onRESTQuery(x, QueryType.GET));
		router.delete("/deletedocument/*").handler(x -> this.onRESTQuery(x, QueryType.DELETEDOCUMENT));
		
		listeningServer = getVertx().createHttpServer(options);
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
		tcpClient.processQuery(query, callback);
	}
	
	public void onRESTQuery(RoutingContext routingContext, QueryType type) {
		HttpServerResponse response = routingContext.response();
		HttpServerRequest request = routingContext.request();
		String path = request.path();
		
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

	
	public static void main(String[] args) {
		RESTQueryInterface RESTInterface = new RESTQueryInterface("localhost", 6666, 8080);
		RESTInterface.listen();
	}
}
