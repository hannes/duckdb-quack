#pragma once

#include "duckdb/catalog/catalog.hpp"

namespace duckdb {
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
