#include "storage/quack_transaction.hpp"
#include "storage/quack_catalog.hpp"

namespace duckdb {

QuackTransaction::QuackTransaction(QuackCatalog &quack_catalog_p, TransactionManager &manager_p,
                                   ClientContext &context_p)
    : Transaction(manager_p, context_p), quack_catalog(quack_catalog_p) {
}

void QuackTransaction::Start() {
	quack_catalog.ExecuteCommand("BEGIN TRANSACTION");
}

void QuackTransaction::Commit() {
	quack_catalog.ExecuteCommand("COMMIT");
}

void QuackTransaction::Rollback() {
	quack_catalog.ExecuteCommand("ROLLBACK");
}

QuackTransaction &QuackTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<QuackTransaction>();
}

QuackTransaction::~QuackTransaction() {
}

} // namespace duckdb
