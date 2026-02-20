#include "catalog.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/storage/database_size.hpp"

using namespace duckdb;

RpcCatalog::RpcCatalog(AttachedDatabase &db_p, const string &server_string) : Catalog(db_p) {
	throw NotImplementedException("RpcCatalog not implemented yet");
}
RpcCatalog::~RpcCatalog() {
}

void RpcCatalog::Initialize(bool load_builtin) {
	throw NotImplementedException("Initialize not implemented yet");
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
	throw NotImplementedException("LookupSchema not implemented yet");
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
