//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/quack_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/transaction/transaction.hpp"

#include "quack_uri.hpp"

namespace duckdb {

class QuackCatalog;

class QuackTransaction : public Transaction {
public:
	QuackTransaction(QuackCatalog &quack_catalog_p, TransactionManager &manager_p, ClientContext &context_p);
	~QuackTransaction() override;
	// TODO
	void Start();
	void Commit();
	void Rollback();
	//
	// optional_ptr<CatalogEntry> GetCatalogEntry(const string &table_name);
	// void DropEntry(CatalogType type, const string &table_name, bool cascade);
	// void ClearTableEntry(const string &table_name);
	//
	static QuackTransaction &Get(ClientContext &context, Catalog &catalog);

private:
	QuackCatalog &quack_catalog;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> catalog_entries;
};

} // namespace duckdb
