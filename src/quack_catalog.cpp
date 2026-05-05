#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/database_size.hpp"

#include "quack_catalog.hpp"
#include "quack_scan.hpp"
#include "quack_insert.hpp"
#include "quack_message.hpp"
#include "quack_client.hpp"

// FIXME bunch of stuff copied from postgres scanner, can probably be simplified!

using namespace duckdb;

QuackTransaction::QuackTransaction(QuackCatalog &rpc_catalog_p, TransactionManager &manager_p, ClientContext &context_p)
    : Transaction(manager_p, context_p), rpc_catalog(rpc_catalog_p) {
}

void QuackTransaction::Start() {
	rpc_catalog.ExecuteCommand("BEGIN TRANSACTION");
}

void QuackTransaction::Commit() {
	rpc_catalog.ExecuteCommand("COMMIT");
}

void QuackTransaction::Rollback() {
	rpc_catalog.ExecuteCommand("ROLLBACK");
}

QuackTransaction &QuackTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<QuackTransaction>();
}

QuackTransaction::~QuackTransaction() {
}

QuackTransactionManager::QuackTransactionManager(AttachedDatabase &db_p, QuackCatalog &rpc_catalog_p)
    : TransactionManager(db_p), rpc_catalog(rpc_catalog_p) {
}

Transaction &QuackTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<QuackTransaction>(rpc_catalog, *this, context);
	transaction->Start();
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}
ErrorData QuackTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &rpc_transaction = transaction.Cast<QuackTransaction>();
	rpc_transaction.Commit();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void QuackTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &rpc_transaction = transaction.Cast<QuackTransaction>();
	rpc_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void QuackTransactionManager::Checkpoint(ClientContext &context, bool force) {
	throw NotImplementedException("Checkpoint not implemented yet");
}

QuackCatalog::QuackCatalog(AttachedDatabase &db_p, const QuackUri &server_uri_p, ClientContext &context)
    : Catalog(db_p), server_uri(server_uri_p), client(QuackClient::GetClient(context, server_uri)) {
	// Resolve auth token: prefer a quack secret scoped to this URI; fall back to the
	// global rpc_default_token setting.
	string token;
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto match = secret_manager.LookupSecret(transaction, server_uri.Uri(), "quack");
	if (match.HasMatch()) {
		const auto &kv = dynamic_cast<const KeyValueSecret &>(*match.secret_entry->secret);
		token = kv.TryGetValue("token", true).ToString();
	} else {
		Value default_token_val;
		auto &config = DBConfig::GetConfig(db_p.GetDatabase());
		auto lookup_result_token = config.TryGetCurrentSetting("rpc_default_token", default_token_val);
		D_ASSERT(lookup_result_token);
		if (!default_token_val.IsNull()) {
			token = default_token_val.GetValue<string>();
		}
	}

	auto connection_response = client->Request<ConnectionResponseMessage>(make_uniq<ConnectionRequestMessage>(token));
	connection_id = connection_response->ConnectionId();

	// TODO a tiiny bit clunky this
	auto schemata = ExecuteCommand("FROM information_schema.schemata SELECT catalog_name, schema_name WHERE "
	                               "catalog_name NOT IN ('system', 'temp') ORDER BY ALL");
	auto row_collection = make_uniq<ColumnDataRowCollection>(schemata->GetRows());
	for (idx_t row_idx = 0; row_idx < row_collection->size(); row_idx++) {
		QuackSchemaInfo info;
		info.catalog_name = row_collection->GetValue(0, row_idx).GetValue<string>();
		info.schema_name = row_collection->GetValue(1, row_idx).GetValue<string>();
		// TODO this will fail if there are two schemas with the same name in different catalogs :/
		schemas[info.schema_name] = make_uniq<QuackSchemaCatalogEntry>(*this, info);
	}
}

QuackCatalog::~QuackCatalog() {
}

void QuackCatalog::Initialize(bool load_builtin) {
}

optional_ptr<SchemaCatalogEntry> QuackCatalog::LookupSchema(CatalogTransaction transaction,
                                                            const EntryLookupInfo &schema_lookup,
                                                            OnEntryNotFound if_not_found) {
	auto schema_name = schema_lookup.GetEntryName();
	if (schemas.find(schema_name) == schemas.end()) {
		switch (if_not_found) {
		case OnEntryNotFound::THROW_EXCEPTION:
			throw BinderException("Schema with name \"%s\" not found", schema_name);
		case OnEntryNotFound::RETURN_NULL:
			return nullptr;
		}
	}
	return schemas[schema_name].get();
}

const QuackUri &QuackCatalog::GetServerUri() {
	return server_uri;
}

unique_ptr<ColumnDataCollection> QuackCatalog::ExecuteCommand(const string &query) {
	// FIXME this will break with many results!
	auto chunk_collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator());
	auto response =
	    client->Request<PrepareResponseMessage>(make_uniq<PrepareRequestMessage>(connection_id, query, true));
	chunk_collection->Initialize(response->Types());
	for (auto &chunk : response->MutableResults()) {
		chunk_collection->Append(*chunk);
	}
	return chunk_collection;
}

QuackClient &QuackCatalog::GetRawClient() {
	return *client;
}

const string &QuackCatalog::GetConnectionId() {
	return connection_id;
}

optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::LookupEntry(CatalogTransaction transaction,
                                                                const EntryLookupInfo &lookup_info) {
	auto &schema_create_info = GetInfo()->Cast<QuackSchemaInfo>();

	CreateTableInfo create_info(*this, lookup_info.GetEntryName());
	auto &rpc_catalog = catalog.Cast<QuackCatalog>();
	auto catalog_type = lookup_info.GetCatalogType();
	auto &entry_name = lookup_info.GetEntryName();
	if (catalog_type == CatalogType::TABLE_FUNCTION_ENTRY) {
		auto entry = TryLoadBuiltInFunction(entry_name);
		if (entry) {
			return entry;
		}
	}

	try {
		auto bind_response =
		    rpc_catalog.GetRawClient().Request<PrepareResponseMessage>(make_uniq<PrepareRequestMessage>(
		        rpc_catalog.GetConnectionId(), StringUtil::Format("FROM %s WHERE FALSE", lookup_info.GetEntryName()),
		        true));
		for (idx_t i = 0; i < bind_response->Types().size(); i++) {
			create_info.columns.AddColumn(ColumnDefinition(bind_response->Names()[i], bind_response->Types()[i]));
		}
		return new QuackTableCatalogEntry(catalog, *this, create_info);
	} catch (IOException &ex) { // FIXME this should not be a catch on IOError
		return nullptr;
	}
}

TableFunction QuackTableCatalogEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data_p) {
	auto &rpc_catalog = catalog.Cast<QuackCatalog>();
	auto bind_data = make_uniq<QuackScanBindData>();
	bind_data->server_uri = rpc_catalog.GetServerUri();
	bind_data->connection_id = rpc_catalog.GetConnectionId();
	bind_data->table_name = name;
	for (auto &col : GetColumns().Physical()) {
		bind_data->column_names.push_back(col.Name());
		bind_data->column_types.push_back(col.Type());
	}
	bind_data_p = std::move(bind_data);
	return QuackScanFunction::GetFunction();
}

optional_ptr<CatalogEntry> QuackCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	auto create_schema_info = info.Copy();
	create_schema_info->catalog = "memory";

	auto catalog_request_message = make_uniq<CatalogRequestMessage>(GetConnectionId(), std::move(create_schema_info));
	auto catalog_response = GetRawClient().Request<CatalogResponseMessage>(std::move(catalog_request_message));
	return make_uniq_base<CatalogEntry, QuackSchemaCatalogEntry>(
	    *this, catalog_response->GetParseInfo()->Cast<CreateSchemaInfo>());
}

void QuackCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	for (auto &schema : schemas) {
		// callback(*schema.second);
	}
}

PhysicalOperator &QuackCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                           PhysicalOperator &plan) {
	throw NotImplementedException("PlanDelete not implemented yet");
}
PhysicalOperator &QuackCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                           PhysicalOperator &plan) {
	throw NotImplementedException("PlanUpdate not implemented yet");
}

unique_ptr<LogicalOperator> QuackCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                          TableCatalogEntry &table, unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("BindCreateIndex not implemented yet");
}

DatabaseSize QuackCatalog::GetDatabaseSize(ClientContext &context) {
	throw NotImplementedException("GetDatabaseSize not implemented yet");
}

//! Whether or not this is an in-memory SQLite database
bool QuackCatalog::InMemory() {
	throw NotImplementedException("InMemory not implemented yet");
}
string QuackCatalog::GetDBPath() {
	throw NotImplementedException("GetDBPath not implemented yet");
}

void QuackCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	// TODO should we just send over the drop info in a dropmessage???
	throw NotImplementedException("DropSchema not implemented yet");
}

void QuackSchemaCatalogEntry::Scan(ClientContext &context, CatalogType type,
                                   const std::function<void(CatalogEntry &)> &callback) {
	// TODO
}
void QuackSchemaCatalogEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	// TODO
}

optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                                TableCatalogEntry &table) {
	throw NotImplementedException("CreateIndex not implemented yet");
}
optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::CreateFunction(CatalogTransaction transaction,
                                                                   CreateFunctionInfo &info) {
	throw NotImplementedException("CreateFunction not implemented yet");
}

optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::CreateTable(CatalogTransaction transaction,
                                                                BoundCreateTableInfo &info) {
	auto &rpc_catalog = catalog.Cast<QuackCatalog>();

	auto create_table_info = info.Base().Copy();
	create_table_info->catalog = GetInfo()->catalog;
	create_table_info->schema = GetInfo()->schema;

	auto catalog_request_message =
	    make_uniq<CatalogRequestMessage>(rpc_catalog.GetConnectionId(), std::move(create_table_info));
	auto catalog_response =
	    rpc_catalog.GetRawClient().Request<CatalogResponseMessage>(std::move(catalog_request_message));
	return make_uniq_base<CatalogEntry, QuackTableCatalogEntry>(
	    catalog, *this, catalog_response->GetParseInfo()->Cast<CreateTableInfo>());
}

optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw NotImplementedException("CreateView not implemented yet");
}
optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::CreateSequence(CatalogTransaction transaction,
                                                                   CreateSequenceInfo &info) {
	throw NotImplementedException("CreateSequence not implemented yet");
}
optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                        CreateTableFunctionInfo &info) {
	throw NotImplementedException("CreateTableFunction not implemented yet");
}
optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                       CreateCopyFunctionInfo &info) {
	throw NotImplementedException("CreateCopyFunction not implemented yet");
}
optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                         CreatePragmaFunctionInfo &info) {
	throw NotImplementedException("CreatePragmaFunction not implemented yet");
}
optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::CreateCollation(CatalogTransaction transaction,
                                                                    CreateCollationInfo &info) {
	throw NotImplementedException("CreateCollation not implemented yet");
}

optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw NotImplementedException("CreateType not implemented yet");
}

void QuackSchemaCatalogEntry::DropEntry(ClientContext &context, DropInfo &info_p) {
	auto &rpc_catalog = catalog.Cast<QuackCatalog>();
	auto drop_info = info_p.Copy();
	drop_info->catalog = GetInfo()->catalog;
	drop_info->schema = GetInfo()->schema;

	auto catalog_request_message =
	    make_uniq<CatalogRequestMessage>(rpc_catalog.GetConnectionId(), std::move(drop_info));

	rpc_catalog.GetRawClient().Request<CatalogResponseMessage>(std::move(catalog_request_message));
}
void QuackSchemaCatalogEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw NotImplementedException("Alter not implemented yet, Alter!");
}

unique_ptr<BaseStatistics> QuackTableCatalogEntry::GetStatistics(ClientContext &context, column_t column_id) {
	throw NotImplementedException("GetStatistics not implemented yet");
}

TableStorageInfo QuackTableCatalogEntry::GetStorageInfo(ClientContext &context) {
	throw NotImplementedException("GetStorageInfo not implemented yet");
}

// clang-format off
static const DefaultTableMacro rpc_table_macros[] = {
	{DEFAULT_SCHEMA, "call", {"remote_sql_query", nullptr}, {{nullptr, nullptr}},  "FROM rpc_call_by_name({CATALOG}, remote_sql_query)"},
	{nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}
};
// clang-format on

// 'borrowed' from ducklake
optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::LoadBuiltInFunction(DefaultTableMacro macro) {
	string macro_def = macro.macro;
	macro_def = StringUtil::Replace(macro_def, "{CATALOG}", KeywordHelper::WriteQuoted(catalog.GetName(), '\''));
	macro_def = StringUtil::Replace(macro_def, "{SCHEMA}", KeywordHelper::WriteQuoted(name, '\''));
	macro.macro = macro_def.c_str();
	auto info = DefaultTableFunctionGenerator::CreateTableMacroInfo(macro);
	auto table_macro =
	    make_uniq_base<CatalogEntry, TableMacroCatalogEntry>(catalog, *this, info->Cast<CreateMacroInfo>());
	auto result = table_macro.get();
	default_function_map.emplace(macro.name, std::move(table_macro));
	return result;
}

optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::TryLoadBuiltInFunction(const string &entry_name) {
	lock_guard<mutex> guard(default_function_lock);
	auto entry = default_function_map.find(entry_name);
	if (entry != default_function_map.end()) {
		return entry->second.get();
	}
	for (idx_t index = 0; rpc_table_macros[index].name != nullptr; index++) {
		if (StringUtil::CIEquals(rpc_table_macros[index].name, entry_name)) {
			return LoadBuiltInFunction(rpc_table_macros[index]);
		}
	}
	return nullptr;
}
