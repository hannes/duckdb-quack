#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {

class RpcCatalog;

enum class RpcTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

class RpcTransaction : public Transaction {
public:
	RpcTransaction(RpcCatalog &rpc_catalog_p, TransactionManager &manager_p, ClientContext &context_p);
	~RpcTransaction() override;
	// TODO
	void Start();
	void Commit();
	void Rollback();
	//
	// optional_ptr<CatalogEntry> GetCatalogEntry(const string &table_name);
	// void DropEntry(CatalogType type, const string &table_name, bool cascade);
	// void ClearTableEntry(const string &table_name);
	//
	static RpcTransaction &Get(ClientContext &context, Catalog &catalog);

private:
	RpcCatalog &rpc_catalog;
	RpcTransactionState transaction_state;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> catalog_entries;
};

class RpcTransactionManager : public TransactionManager {
public:
	RpcTransactionManager(AttachedDatabase &db_p, RpcCatalog &sqlite_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	RpcCatalog &rpc_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<RpcTransaction>> transactions;
};

class RpcCatalog : public Catalog {
public:
	explicit RpcCatalog(AttachedDatabase &db_p, const string &server_string);
	~RpcCatalog();

public:
	string GetCatalogType() override {
		return "rpc";
	}
	void Initialize(bool load_builtin) override;

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;

	//! Whether or not this is an in-memory SQLite database
	bool InMemory() override;
	string GetDBPath() override;

	// //! Returns a reference to the in-memory database (if any)
	// SQLiteDB *GetInMemoryDatabase();
	// //! Release the in-memory database (if there is any)
	// void ReleaseInMemoryDatabase();

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;
};

} // namespace duckdb
