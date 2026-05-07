//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/quack_schema.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

class QuackCatalog;

class QuackTableCatalogEntry : public TableCatalogEntry {
public:
	QuackTableCatalogEntry(Catalog &catalog_p, SchemaCatalogEntry &schema_p, CreateTableInfo &info_p)
	    : TableCatalogEntry(catalog_p, schema_p, info_p) {
	}

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
};

} // namespace duckdb
