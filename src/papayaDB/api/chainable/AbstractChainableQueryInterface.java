package papayaDB.api.chainable;

import papayaDB.api.QueryInterface;

/**
 * Cette classe abstraite implémente les concepts communs aux "interfaces chainées" : des interfaces de requête pouvant traiter celles qu'elles reçoivent en les renvoyant vers d'autres (ou pas).
 * On peut considérer cette classe comme la classe la plus adaptée à étendre pour créer ses propres QueryInterface.
 * Cette implémentation définit le fonctionnement d'une AbstractChainableQueryInterface.
 */
public class AbstractChainableQueryInterface implements QueryInterface {
	
}
