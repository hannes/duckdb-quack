#include "storage/quack_catalog.hpp"
#include "storage/quack_table.hpp"
#include "quack_scan.hpp"

namespace duckdb {

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

} // namespace duckdb
