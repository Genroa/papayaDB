package papayaDB.api.chainable;

import java.util.function.Consumer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import papayaDB.api.QueryAnswer;
import papayaDB.api.QueryInterface;

/**
 * Cette classe abstraite implémente les concepts communs aux "interfaces chainées" : des interfaces de requête pouvant traiter celles qu'elles reçoivent en les renvoyant vers d'autres (ou pas).
 * On peut considérer cette classe comme la classe la plus adaptée à étendre pour créer ses propres QueryInterface.
 * Cette implémentation définit le fonctionnement d'une AbstractChainableQueryInterface.
 */
public abstract class AbstractChainableQueryInterface extends AbstractVerticle implements QueryInterface {
	/**
	 * Objet {@link Vertx}
	 */
	private final Vertx vertx;
	
	/**
	 * Constructeur de la classe abstraite.
	 * Créé un Objet {@link Vertx} qui sera stocké. 
	 */
	public AbstractChainableQueryInterface() {
		vertx = Vertx.vertx();
	}

	@Override
	public void processQuery(String query, Consumer<QueryAnswer> callback) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Ferme définitivement les connexions et rend l'objet inutilisable. A appeler impérativement après utilisation.
	 * En cas d'oubli, les threads de Vertx maintiendront la JVM en vie.
	 */
	public void close() {
		vertx.close();
	}
	
	/**
	 * Méthode permettant de récuperer l'objet {@link Vertx} instancié.
	 * @return Vertx L'objet {@link Vertx}
	 */
	public Vertx getVertx() {
		return vertx;
	}
}