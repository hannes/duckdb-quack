#include "catalog.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/storage/database_size.hpp"

// FIXME bunch of stuff copied from postgres scanner, can probably be simplified!

using namespace duckdb;

RpcTransaction::RpcTransaction(RpcCatalog &rpc_catalog_p, TransactionManager &manager_p, ClientContext &context_p)
    : Transaction(manager_p, context_p), rpc_catalog(rpc_catalog_p) {
}

void RpcTransaction::Start() {
	transaction_state = RpcTransactionState::TRANSACTION_NOT_YET_STARTED;
}

void RpcTransaction::Commit() {
	throw NotImplementedException("Commit not implemented yet");

	// if (transaction_state == RpcTransactionState::TRANSACTION_STARTED) {
	// 	transaction_state = RpcTransactionState::TRANSACTION_FINISHED;
	// 	GetConnectionRaw().Execute(context, "COMMIT");
	// }
}

void RpcTransaction::Rollback() {
	throw NotImplementedException("Rollback not implemented yet");
	//
	// if (transaction_state == RpcTransactionState::TRANSACTION_STARTED) {
	// 	transaction_state = RpcTransactionState::TRANSACTION_FINISHED;
	// 	GetConnectionRaw().Execute(context, "ROLLBACK");
	// }
}
//
// PostgresConnection &RpcTransaction::GetConnection() {
// 	auto &con = GetConnectionRaw();
// 	if (transaction_state == RpcTransactionState::TRANSACTION_NOT_YET_STARTED) {
// 		transaction_state = RpcTransactionState::TRANSACTION_STARTED;
// 		con.Execute(GetContext(), "BEGIN");
// 	}
// 	return con;
// }
//
// PostgresConnection &RpcTransaction::GetConnectionRaw() {
// 	return connection.GetConnection();
// }
//

RpcTransaction &RpcTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<RpcTransaction>();
}

RpcTransaction::~RpcTransaction() {
}

RpcTransactionManager::RpcTransactionManager(AttachedDatabase &db_p, RpcCatalog &rpc_catalog_p)
    : TransactionManager(db_p), rpc_catalog(rpc_catalog_p) {
}

Transaction &RpcTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<RpcTransaction>(rpc_catalog, *this, context);
	transaction->Start();
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}
ErrorData RpcTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &rpc_transaction = transaction.Cast<RpcTransaction>();
	rpc_transaction.Commit();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void RpcTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &rpc_transaction = transaction.Cast<RpcTransaction>();
	rpc_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void RpcTransactionManager::Checkpoint(ClientContext &context, bool force) {
	throw NotImplementedException("Checkpoint not implemented yet");
	//
	// auto &transaction = RpcTransaction::Get(context, db.GetCatalog());
	// auto &db = transaction.GetConnection();
	// db.Execute(context, "CHECKPOINT");
}

RpcCatalog::RpcCatalog(AttachedDatabase &db_p, const string &server_string) : Catalog(db_p) {
}
RpcCatalog::~RpcCatalog() {
}

void RpcCatalog::Initialize(bool load_builtin) {
}

optional_ptr<CatalogEntry> RpcCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	throw NotImplementedException("~RpcCatalog not implemented yet");
}

void RpcCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	throw NotImplementedException("ScanSchemas not implemented yet");
}

optional_ptr<SchemaCatalogEntry> RpcCatalog::LookupSchema(CatalogTransaction transaction,
                                                          const EntryLookupInfo &schema_lookup,
                                                          OnEntryNotFound if_not_found) {
	auto schema_name = schema_lookup.GetEntryName();
	auto &rpc_transaction = RpcTransaction::Get(transaction.GetContext(), *this);
	// FIXME
	// auto entry = schemas.GetEntry(transaction.GetContext(), rpc_transaction, schema_name);
	optional_ptr<SchemaCatalogEntry> entry = nullptr;
	if (!entry && if_not_found != OnEntryNotFound::RETURN_NULL) {
		throw BinderException("Schema with name \"%s\" not found", schema_name);
	}
	return reinterpret_cast<SchemaCatalogEntry *>(entry.get());
}

PhysicalOperator &RpcCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalCreateTable &op, PhysicalOperator &plan) {
	throw NotImplementedException("PlanCreateTableAs not implemented yet");
}
PhysicalOperator &RpcCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                         optional_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("PlanInsert not implemented yet");
}
PhysicalOperator &RpcCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                         PhysicalOperator &plan) {
	throw NotImplementedException("PlanDelete not implemented yet");
}
PhysicalOperator &RpcCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                         PhysicalOperator &plan) {
	throw NotImplementedException("PlanUpdate not implemented yet");
}

unique_ptr<LogicalOperator> RpcCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
                                                        unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("BindCreateIndex not implemented yet");
}

DatabaseSize RpcCatalog::GetDatabaseSize(ClientContext &context) {
	throw NotImplementedException("GetDatabaseSize not implemented yet");
}

//! Whether or not this is an in-memory SQLite database
bool RpcCatalog::InMemory() {
	throw NotImplementedException("InMemory not implemented yet");
}
string RpcCatalog::GetDBPath() {
	throw NotImplementedException("GetDBPath not implemented yet");
}

void RpcCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("DropSchema not implemented yet");
}
