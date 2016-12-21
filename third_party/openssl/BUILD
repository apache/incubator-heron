licenses(["notice"])

package(default_visibility = ["//visibility:public"])

package_name = "openssl"
package_version = "1.1.0c"

package_file = package_name + "-" + package_version + ".tar.gz"
package_dir = package_name + "-" + package_version

include_files = [
    "include/openssl/aes.h",
    "include/openssl/asn1.h",
    "include/openssl/asn1_mac.h",
    "include/openssl/asn1t.h",
    "include/openssl/async.h",
    "include/openssl/bio.h",
    "include/openssl/blowfish.h",
    "include/openssl/bn.h",
    "include/openssl/buffer.h",
    "include/openssl/camellia.h",
    "include/openssl/cast.h",
    "include/openssl/cmac.h",
    "include/openssl/cms.h",
    "include/openssl/comp.h",
    "include/openssl/conf.h",
    "include/openssl/conf_api.h",
    "include/openssl/crypto.h",
    "include/openssl/ct.h",
    "include/openssl/des.h",
    "include/openssl/dh.h",
    "include/openssl/dsa.h",
    "include/openssl/dtls1.h",
    "include/openssl/e_os2.h",
    "include/openssl/ebcdic.h",
    "include/openssl/ec.h",
    "include/openssl/ecdh.h",
    "include/openssl/ecdsa.h",
    "include/openssl/engine.h",
    "include/openssl/err.h",
    "include/openssl/evp.h",
    "include/openssl/hmac.h",
    "include/openssl/idea.h",
    "include/openssl/kdf.h",
    "include/openssl/lhash.h",
    "include/openssl/md2.h",
    "include/openssl/md4.h",
    "include/openssl/md5.h",
    "include/openssl/mdc2.h",
    "include/openssl/modes.h",
    "include/openssl/obj_mac.h",
    "include/openssl/objects.h",
    "include/openssl/ocsp.h",
    "include/openssl/opensslconf.h",
    "include/openssl/opensslv.h",
    "include/openssl/ossl_typ.h",
    "include/openssl/pem.h",
    "include/openssl/pem2.h",
    "include/openssl/pkcs12.h",
    "include/openssl/pkcs7.h",
    "include/openssl/rand.h",
    "include/openssl/rc2.h",
    "include/openssl/rc4.h",
    "include/openssl/rc5.h",
    "include/openssl/ripemd.h",
    "include/openssl/rsa.h",
    "include/openssl/safestack.h",
    "include/openssl/seed.h",
    "include/openssl/sha.h",
    "include/openssl/srp.h",
    "include/openssl/srtp.h",
    "include/openssl/ssl.h",
    "include/openssl/ssl2.h",
    "include/openssl/ssl3.h",
    "include/openssl/stack.h",
    "include/openssl/symhacks.h",
    "include/openssl/tls1.h",
    "include/openssl/ts.h",
    "include/openssl/txt_db.h",
    "include/openssl/ui.h",
    "include/openssl/whrlpool.h",
    "include/openssl/x509.h",
    "include/openssl/x509_vfy.h",
    "include/openssl/x509v3.h",
]

lib_files = [
    "lib/libcrypto.a",
    "lib/libssl.a",
]

genrule(
    name = "openssl-srcs",
    srcs = [
        package_file,
    ],
    outs = include_files + lib_files, 
    cmd = "\n".join([
        "export INSTALL_DIR=$$(pwd)/$(@D)",
        "export TMP_DIR=$$(mktemp -d -t openssl.XXXXX)",
        "mkdir -p $$TMP_DIR",
        "cp -R $(SRCS) $$TMP_DIR",
        "cd $$TMP_DIR",
        "tar xfz " + package_file,
        "cd " + package_dir,
        "KERNEL_BITS=64 ./config --prefix=$$INSTALL_DIR no-shared",
        "make install",
        "rm -rf $$TMP_DIR",
    ]),
)

cc_library(
    name = "openssl-cxx",
    srcs = [
        "empty.cc",
    ] + lib_files,
    hdrs = include_files,
    includes = [
        "include",
    ],
    linkstatic = 1,
)

filegroup(
    name = "openssl-files",
    srcs = include_files + lib_files,
)
