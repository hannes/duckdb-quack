#include <string>
#include <functional>

#include <openssl/encoder.h>
#include <openssl/evp.h>
#include <openssl/dh.h>
#include <openssl/rsa.h>
#include <openssl/x509.h>
#include <openssl/pem.h>

#include "ssl_key_generator.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/file_system.hpp"

using namespace duckdb;

std::string SslKeyGenerator::GetDefaultCertificateDirectory(FileSystem &fs) {
	auto certificate_directory = fs.JoinPath(fs.GetHomeDirectory(), ".duckdb", "extension_data", "quack");
	if (!fs.DirectoryExists(certificate_directory)) {
		fs.CreateDirectoriesRecursive(certificate_directory);
	}
	D_ASSERT(fs.DirectoryExists(certificate_directory));
	return certificate_directory;
}

const int RSA_KEY_LENGTH = 4096;
const int DH_PARAM_LENGTH = 2048;

void SslKeyGenerator::GenerateSslKeys(const std::string &cert_filename, const std::string &private_key_filename,
                                      const std::string &dh_filename, size_t cert_days_valid) {
	// some C++ magic to clean openssl stuff up sanely

	std::function<EVP_PKEY_CTX *(int)> create_context = [](int id) -> EVP_PKEY_CTX * {
		auto ctx = EVP_PKEY_CTX_new_id(id, NULL);
		if (!ctx) {
			throw std::runtime_error("Error creating key context");
		}
		return ctx;
	};

	std::function<OSSL_ENCODER_CTX *(EVP_PKEY *, int)> create_encoder = [&](EVP_PKEY *key,
	                                                                        int selection) -> OSSL_ENCODER_CTX * {
		auto encoder = OSSL_ENCODER_CTX_new_for_pkey(key, selection, "PEM", NULL, NULL);
		if (!encoder) {
			throw std::runtime_error("Error creating encoder context");
		}
		return encoder;
	};

	std::function<BIO *(const std::string &)> create_bio = [](const std::string &filename) -> BIO * {
		auto bio = BIO_new_file(filename.data(), "wb");
		if (!bio) {
			throw std::runtime_error("Error creating bio");
		}
		return bio;
	};

	// create rsa & certificate
	{
		std::unique_ptr<EVP_PKEY_CTX, void (*)(EVP_PKEY_CTX *)> rsa_ctx {create_context(EVP_PKEY_RSA),
		                                                                 EVP_PKEY_CTX_free};

		EVP_PKEY *rsa_ptr = nullptr;
		if (EVP_PKEY_keygen_init(rsa_ctx.get()) <= 0 ||
		    EVP_PKEY_CTX_set_rsa_keygen_bits(rsa_ctx.get(), RSA_KEY_LENGTH) <= 0 ||
		    EVP_PKEY_keygen(rsa_ctx.get(), &rsa_ptr) <= 0) {
			throw std::runtime_error("Error creating RSA parameters");
		}
		std::unique_ptr<EVP_PKEY, void (*)(EVP_PKEY *)> rsa {rsa_ptr, EVP_PKEY_free};

		std::unique_ptr<OSSL_ENCODER_CTX, void (*)(OSSL_ENCODER_CTX *)> rsa_encoder {
		    create_encoder(rsa.get(), OSSL_KEYMGMT_SELECT_PRIVATE_KEY), OSSL_ENCODER_CTX_free};
		std::unique_ptr<BIO, void (*)(BIO *)> rsa_file {create_bio(private_key_filename), BIO_free_all};
		if (!OSSL_ENCODER_to_bio(rsa_encoder.get(), rsa_file.get())) {
			throw std::runtime_error("Error serializing RSA");
		}

		std::unique_ptr<X509, void (*)(X509 *)> cert {X509_new(), X509_free};

		// some cert config
		ASN1_INTEGER_set(X509_get_serialNumber(cert.get()), 1);
		X509_gmtime_adj(X509_get_notBefore(cert.get()), 0);
		X509_gmtime_adj(X509_get_notAfter(cert.get()), cert_days_valid * 24 * 3600);
		X509_name_st *name = X509_get_subject_name(cert.get());

		const unsigned char common_name[] = "localhost";

		// TODO do we need this other stuff?
		// X509_NAME_add_entry_by_txt(name, "C", MBSTRING_ASC, country, -1, -1, 0);
		// X509_NAME_add_entry_by_txt(name, "O", MBSTRING_ASC, company, -1, -1, 0);
		X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC, common_name, -1, -1, 0);

		if (X509_set_issuer_name(cert.get(), name) != 1 || X509_set_pubkey(cert.get(), rsa.get()) != 1 ||
		    X509_sign(cert.get(), rsa.get(), EVP_sha256()) == 0) {
			throw std::runtime_error("Error creating certificate");
		}
		std::unique_ptr<BIO, void (*)(BIO *)> cert_file {create_bio(cert_filename), BIO_free_all};
		if (!PEM_write_bio_X509(cert_file.get(), cert.get())) {
			throw std::runtime_error("Error serializing certificate");
		}
	}

	if (false) { // TODO this seems to be not needed atm
		// create dh parameters
		std::unique_ptr<EVP_PKEY_CTX, void (*)(EVP_PKEY_CTX *)> dh_ctx {create_context(EVP_PKEY_DH), EVP_PKEY_CTX_free};

		EVP_PKEY *dh_ptr = nullptr;
		if (EVP_PKEY_paramgen_init(dh_ctx.get()) <= 0 ||
		    EVP_PKEY_CTX_set_dh_paramgen_prime_len(dh_ctx.get(), DH_PARAM_LENGTH) <= 0 ||
		    EVP_PKEY_paramgen(dh_ctx.get(), &dh_ptr) <= 0) {
			throw std::runtime_error("Error creating DH parameters");
		}
		std::unique_ptr<EVP_PKEY, void (*)(EVP_PKEY *)> dh {dh_ptr, EVP_PKEY_free};

		std::unique_ptr<OSSL_ENCODER_CTX, void (*)(OSSL_ENCODER_CTX *)> dh_encoder {
		    create_encoder(dh.get(), OSSL_KEYMGMT_SELECT_DOMAIN_PARAMETERS), OSSL_ENCODER_CTX_free};
		std::unique_ptr<BIO, void (*)(BIO *)> dh_file {create_bio(dh_filename), BIO_free_all};

		if (!OSSL_ENCODER_to_bio(dh_encoder.get(), dh_file.get())) {
			throw std::runtime_error("Error serializing DH");
		}
	}
}
