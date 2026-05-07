#include "storage/quack_catalog.hpp"
#include "storage/quack_table.hpp"
#include "quack_scan.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {

unique_ptr<CreateInfo> ParseCreateTable(const string &sql) {
	Parser parser;
	parser.ParseQuery(sql);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::CREATE_STATEMENT) {
		throw BinderException(
		    "Failed to create view from SQL string - \"%s\" - statement did not contain a single SELECT statement",
		    sql);
	}
	auto &create = parser.statements[0]->Cast<CreateStatement>();
	return std::move(create.info);
}

QuackTableSet::QuackTableSet(ClientContext &context, QuackSchemaCatalogEntry &parent,
                             const QuackLoadCatalogData &load_data)
    : QuackCatalogSet(parent.ParentCatalog().Cast<QuackCatalog>()), schema(parent) {
	for (auto &row : load_data.tables->Rows()) {
		auto schema_name = row.GetValue(0).GetValue<string>();
		if (schema_name != parent.name) {
			// does not belong to this schema
			continue;
		}
		// parse the SQL to get the table definition
		auto sql = row.GetValue(1).GetValue<string>();
		auto info = ParseCreateTable(sql);
		if (info->type != CatalogType::TABLE_ENTRY) {
			throw InternalException("Expected a CREATE TABLE");
		}
		// bind to resolve the types
		auto binder = Binder::CreateBinder(context);
		auto bound_info = binder->BindCreateTableInfo(std::move(info), schema);
		auto table = make_uniq<QuackTableCatalogEntry>(catalog, parent, bound_info->Base());
		CreateEntry(std::move(table), OnCreateConflict::REPLACE_ON_CONFLICT);
	}
}

string QuackTableSet::GetLoadQuery() {
	return R"(
SELECT schema_name, sql
FROM duckdb_tables()
	)";
}

TableFunction QuackTableCatalogEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data_p) {
	auto &quack_catalog = catalog.Cast<QuackCatalog>();
	auto bind_data = make_uniq<QuackScanBindData>();
	bind_data->server_uri = quack_catalog.GetServerUri();
	bind_data->connection_id = quack_catalog.GetConnectionId();
	bind_data->table_name = name;
	for (auto &col : GetColumns().Physical()) {
		bind_data->column_names.push_back(col.Name());
		bind_data->column_types.push_back(col.Type());
	}
	bind_data_p = std::move(bind_data);
	return QuackScanFunction::GetFunction();
}

unique_ptr<BaseStatistics> QuackTableCatalogEntry::GetStatistics(ClientContext &context, column_t column_id) {
	throw NotImplementedException("GetStatistics not implemented yet");
}

TableStorageInfo QuackTableCatalogEntry::GetStorageInfo(ClientContext &context) {
	throw NotImplementedException("GetStorageInfo not implemented yet");
}

} // namespace duckdb
