package papayaDB.rest;

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
import io.vertx.core.Vertx;
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

	/** Constructeur
	 * @param host
	 * 			Hote sur lequel se connecter pour avoir la base de donées
	 * @param connectionPort
	 * 			Port de connexion de la base de données
	 * @param listeningPort
	 * 			Port d'écoute des requetes
	 */
	public RESTQueryInterface(String host, int connectionPort, int listeningPort) {
		
		HttpServerOptions options = new HttpServerOptions().setSsl(true).setKeyStoreOptions(
				new JksOptions()
					.setPath("keystore.jks")
					.setPassword("papayadb")
				);
		
		tcpClient = QueryInterface.newTcpQueryInterface(host, connectionPort);
		this.listeningPort = listeningPort;
		
		router = Router.router(getVertx());
		router.post("/createdb/*").handler(x -> this.createNewDatabase(x));
		router.post("/insert/*").handler(x -> this.insertNewRecord(x));
		router.post("/update/*").handler(x -> this.updateRecord(x));
		router.delete("/deletedb/*").handler(x -> this.deleteDatabase(x));
		router.get("/exportall/*").handler(x -> this.exportDatabase(x));
		router.get("/get/*").handler(x -> this.getRecords(x));
		router.delete("/delete/*").handler(x -> this.deleteRecords(x));
		
		listeningServer = Vertx.vertx().createHttpServer(options);
	} 
	
	@Override
	public void start() throws Exception {
		listen();
		super.start();
	}
	
	/**
	 * Méthode déclenchant l'écoute du serveur
	 */
	public void listen() {
		listeningServer.requestHandler(router::accept).listen(listeningPort);
		System.out.println("[REST:listen]Now listening for HTTP REST queries...");
	}
	
	/**
	 * Méthode de fermeture du serveur
	 */
	public void close() {
		listeningServer.close();
		tcpClient.close();
	}
	
	public static void main(String[] args) {
		RESTQueryInterface RESTInterface = new RESTQueryInterface("localhost", 6666, 8080);
		RESTInterface.listen();
	}
	
	
	/** Méthode privée appellée pour récuperer le json de requete pour la base de données
	 * @param routingContext
	 * 			Contexte de la route
	 * @param type
	 * 			Type de requete
	 * @return
	 * 			JsonObject contenant le json de requete
	 */
	private JsonObject onRESTQuery(RoutingContext routingContext, QueryType type) {
		HttpServerResponse response = routingContext.response();
		HttpServerRequest request = routingContext.request();
		
		JsonObject json = UrlToQuery.convertToJson(request.path(), type);
		if(json.containsKey("status")) { //Savoir si une erreur à été détectée pendant le parsing de l'URL
			response.putHeader("content-type", "application/json")
			.end(Json.encodePrettily(json));
			return null;
		}
		return json;
	}

	/** Méthode d'appel de création d'une nouvelle base
	 * @param routingContext
	 * 			Contexte de la requete
	 */
	public void createNewDatabase(RoutingContext routingContext) {
		JsonObject json = onRESTQuery(routingContext, QueryType.CREATEDB);
		if(json == null) {
			return;
		}
		tcpClient.createNewDatabase(json.getString("db"),
									json.getJsonObject("auth").getString("user"), 
									json.getJsonObject("auth").getString("hash"), 
									answer -> {
										routingContext.response().putHeader("content-type", "application/json")
										.end(Json.encodePrettily(answer.getData()));
									});
	}

	/** Méthode d'appel de suppression de base de données
	 * @param routingContext
	 * 			Contexte de la requete
	 */
	public void deleteDatabase(RoutingContext routingContext) {
		JsonObject json = onRESTQuery(routingContext, QueryType.DELETEDB);
		if(json == null) {
			return;
		}
		
		tcpClient.deleteDatabase(json.getString("db"),
									json.getJsonObject("auth").getString("user"), 
									json.getJsonObject("auth").getString("hash"), 
									answer -> {
										routingContext.response().putHeader("content-type", "application/json")
										.end(Json.encodePrettily(answer.getData()));
									});
	}

	/** Méthode d'appel d'export de base de données
	 * @param routingContext
	 * 			Contexte de la requete
	 */
	public void exportDatabase(RoutingContext routingContext) {
		JsonObject json = onRESTQuery(routingContext, QueryType.EXPORTALL);
		if(json == null) {
			return;
		}
		
		tcpClient.exportDatabase(json.getString("db"),
									answer -> {
										routingContext.response().putHeader("content-type", "application/json")
										.end(Json.encodePrettily(answer.getData()));
									});
	}

	/**Méthode d'appel de mise à jour de base de données
	 * @param routingContext
	 * 			Contexte de la requete
	 */
	public void updateRecord(RoutingContext routingContext) {
		JsonObject json = onRESTQuery(routingContext, QueryType.UPDATE);
		if(json == null) {
			return;
		}
		
		routingContext.request().bodyHandler(buffer -> {
						JsonObject body = buffer.toJsonObject();
						tcpClient.updateRecord(json.getString("db"),
								body.getString("uid"),
								body.getJsonObject("record"),
								json.getJsonObject("auth").getString("user"), 
								json.getJsonObject("auth").getString("hash"), 
								answer -> {
									routingContext.response().putHeader("content-type", "application/json")
									.end(Json.encodePrettily(answer.getData()));
								});
					});
	}

	/** Méthode d'appel de suppression d'un enregistrement de la base
	 * @param routingContext
	 * 			Contexte de la requete
	 */
	public void deleteRecords(RoutingContext routingContext) {
		JsonObject json = onRESTQuery(routingContext, QueryType.DELETE);
		if(json == null) {
			return;
		}
		
		tcpClient.deleteRecords(json.getString("db"),
								json.getJsonObject("parameters"),
								json.getJsonObject("auth").getString("user"), 
								json.getJsonObject("auth").getString("hash"), 
								answer -> {
									routingContext.response().putHeader("content-type", "application/json")
									.end(Json.encodePrettily(answer.getData()));
								});
	}

	/** Méthode d'appel d'insertion d'un élément dans une base
	 * @param routingContext
	 * 			Contexte de la requete
	 */
	public void insertNewRecord(RoutingContext routingContext) {
		JsonObject json = onRESTQuery(routingContext, QueryType.INSERT);
		if(json == null) {
			return;
		}
		
		routingContext.request().bodyHandler(buffer -> {
			JsonObject body = buffer.toJsonObject();
			tcpClient.insertNewRecord(json.getString("db"),
						body.getJsonObject("record"),
						json.getJsonObject("auth").getString("user"), 
						json.getJsonObject("auth").getString("hash"), 
						answer -> {
							routingContext.response().putHeader("content-type", "application/json")
							.end(Json.encodePrettily(answer.getData()));
						});
		});
	}

	/** Méthode d'appel de requete sur la base
	 * @param routingContext
	 * 			Contexte de la requete
	 */
	public void getRecords(RoutingContext routingContext) {
		System.out.println("[GetRecord:Path]"+routingContext.request().path());
		JsonObject json = onRESTQuery(routingContext, QueryType.GET);
		if(json == null) {
			return;
		}
		
		tcpClient.getRecords(json.getString("db"),
							json.getJsonObject("parameters"),
							answer -> {
								routingContext.response().putHeader("content-type", "application/json")
								.end(Json.encodePrettily(answer.getData()));
							});
	}
}
