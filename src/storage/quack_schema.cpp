#include "storage/quack_catalog.hpp"
#include "storage/quack_schema.hpp"
#include "storage/quack_table.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "quack_client.hpp"

namespace duckdb {

optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::LookupEntry(CatalogTransaction transaction,
                                                                const EntryLookupInfo &lookup_info) {
	auto &schema_create_info = GetInfo()->Cast<QuackSchemaInfo>();

	auto &quack_catalog = catalog.Cast<QuackCatalog>();
	auto catalog_type = lookup_info.GetCatalogType();
	auto &entry_name = lookup_info.GetEntryName();
	if (catalog_type == CatalogType::TABLE_FUNCTION_ENTRY) {
		return TryLoadBuiltInFunction(entry_name);
	}
	if (catalog_type != CatalogType::TABLE_ENTRY) {
		return nullptr;
	}

	CreateTableInfo create_info(*this, lookup_info.GetEntryName());
	try {
		auto bind_response = quack_catalog.GetRawClient().Request<PrepareResponseMessage>(
		    make_uniq<PrepareRequestMessage>(quack_catalog.GetConnectionId(),
		                                     StringUtil::Format("FROM %s WHERE FALSE", lookup_info.GetEntryName())));
		for (idx_t i = 0; i < bind_response->Types().size(); i++) {
			create_info.columns.AddColumn(ColumnDefinition(bind_response->Names()[i], bind_response->Types()[i]));
		}
		return new QuackTableCatalogEntry(catalog, *this, create_info);
	} catch (IOException &ex) { // FIXME this should not be a catch on IOError
		return nullptr;
	}
}

} // namespace duckdb
