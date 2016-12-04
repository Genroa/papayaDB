package papayaDB.rest;

import java.util.function.Consumer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import papayaDB.api.query.QueryAnswer;
import papayaDB.api.query.QueryInterface;
import papayaDB.api.query.QueryType;

/**
 * Représente une interface web d'accès à une base de données, par l'intermédiaire de routes
 * suivant le REST. 
 *
 */
public class RESTQueryInterface extends AbstractVerticle{
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
		router.post("/createdb/*").handler(x -> this.createNewDatabase(x, this::callback));
		router.post("/insert/*").handler(x -> this.onRESTQuery(x, QueryType.INSERT));
		router.post("/update/*").handler(x -> this.onRESTQuery(x, QueryType.UPDATE));
		router.delete("/deletedb/*").handler(x -> this.onRESTQuery(x, QueryType.DELETEDB));
		router.get("/exportall/*").handler(x -> this.onRESTQuery(x, QueryType.EXPORTALL));
		router.get("/get/*").handler(x -> this.onRESTQuery(x, QueryType.GET));
		router.delete("/delete/*").handler(x -> this.onRESTQuery(x, QueryType.DELETE));
		
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
	
	public void close() {
		listeningServer.close();
		tcpClient.close();
	}
	
	public JsonObject getJsonObject(RoutingContext routingContext, QueryType type) {
		HttpServerResponse response = routingContext.response();
		HttpServerRequest request = routingContext.request();
		String path = request.path();
		
		JsonObject json = UrlToQuery.convertToJson(routingContext.request().path(), type);
		if(json.containsKey("status")) { //Savoir si une erreur à été détectée pendant le parsing de l'URL
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

	public void createNewDatabase(RoutingContext routingContext) {
		HttpServerResponse response = routingContext.response();
		HttpServerRequest request = routingContext.request();
		String path = request.path();
		
		JsonObject json = UrlToQuery.convertToJson(routingContext.request().path(), QueryType.CREATEDB);
		if(json.containsKey("status")) { //Savoir si une erreur à été détectée pendant le parsing de l'URL
			response.putHeader("content-type", "application/json")
			.end(Json.encodePrettily(json));
			return;
		}
		
		tcpClient.createNewDatabase(json.getString("db"),
									json.getJsonObject("auth").getString("user"), 
									json.getJsonObject("auth").getString("pass"), 
									answer -> {
										response.putHeader("content-type", "application/json")
										.end(Json.encodePrettily(answer.getData()));
									});
	}

	public void deleteDatabase(RoutingContext routingContext) {
		HttpServerResponse response = routingContext.response();
		HttpServerRequest request = routingContext.request();
		String path = request.path();
		
		JsonObject json = UrlToQuery.convertToJson(routingContext.request().path(), QueryType.DELETEDB);
		if(json.containsKey("status")) { //Savoir si une erreur à été détectée pendant le parsing de l'URL
			response.putHeader("content-type", "application/json")
			.end(Json.encodePrettily(json));
			return;
		}
		
		tcpClient.deleteDatabase(json.getString("db"),
									json.getJsonObject("auth").getString("user"), 
									json.getJsonObject("auth").getString("pass"), 
									answer -> {
										response.putHeader("content-type", "application/json")
										.end(Json.encodePrettily(answer.getData()));
									});
	}

	public void exportDatabase(RoutingContext routingContext) {
		HttpServerResponse response = routingContext.response();
		HttpServerRequest request = routingContext.request();
		String path = request.path();
		
		JsonObject json = UrlToQuery.convertToJson(routingContext.request().path(), QueryType.EXPORTALL);
		if(json.containsKey("status")) { //Savoir si une erreur à été détectée pendant le parsing de l'URL
			response.putHeader("content-type", "application/json")
			.end(Json.encodePrettily(json));
			return;
		}
		
		tcpClient.exportDatabase(json.getString("db"),
									answer -> {
										response.putHeader("content-type", "application/json")
										.end(Json.encodePrettily(answer.getData()));
									});
	}

	public void updateRecord(RoutingContext routingContext) {
		HttpServerResponse response = routingContext.response();
		HttpServerRequest request = routingContext.request();
		String path = request.path();
		
		JsonObject json = UrlToQuery.convertToJson(routingContext.request().path(), QueryType.CREATEDB);
		if(json.containsKey("status")) { //Savoir si une erreur à été détectée pendant le parsing de l'URL
			response.putHeader("content-type", "application/json")
			.end(Json.encodePrettily(json));
			return;
		}
		
		tcpClient.updateRecord(json.getString("db"),
											uid,
											newRecord,
									json.getJsonObject("auth").getString("user"), 
									json.getJsonObject("auth").getString("pass"), 
									answer -> {
										response.putHeader("content-type", "application/json")
										.end(Json.encodePrettily(answer.getData()));
									});
	}

	public void deleteRecords(RoutingContext routingContext) {
		// TODO Auto-generated method stub
		
	}

	public void insertNewRecord(RoutingContext routingContext) {
		// TODO Auto-generated method stub
		
	}

	public void getRecords(RoutingContext routingContext) {
		// TODO Auto-generated method stub
		
	}
}
