package papayaDB.api.query;

/**
 * Définit le status de la requête.
 * Status validé : 		OK
 * Status d'erreur : 	HOST_UNREACHABLE 	==> impossible de contacter l'hote
 * 						SYNTAX_ERROR 		==> problème dans l'écriture de la requête
 */
public enum QueryAnswerStatus {
	OK,
	HOST_UNREACHABLE,
	SYNTAX_ERROR
}