licenses(["notice"])

package(default_visibility = ["//visibility:public"])

package_name = "krb5"
package_version = "1.15"

package_file = package_name + "-" + package_version + ".tar.gz"
package_dir = package_name + "-" + package_version

include_files = [ 
    "include/com_err.h",
    "include/gssapi/gssapi.h",
    "include/gssapi.h",
    "include/kadm5",
    "include/kdb.h",
    "include/krad.h",
    "include/krb5.h",
    "include/profile.h",
    "include/verto-module.h",
    "include/verto.h",
    "include/krb5/ccselect_plugin.h",
    "include/krb5/clpreauth_plugin.h",
    "include/krb5/hostrealm_plugin.h",
    "include/krb5/kadm5_hook_plugin.h",
    "include/krb5/kdcpreauth_plugin.h",
    "include/krb5/krb5.h",
    "include/krb5/localauth_plugin.h",
    "include/krb5/locate_plugin.h",
    "include/krb5/plugin.h",
    "include/krb5/preauth_plugin.h",
    "include/krb5/pwqual_plugin.h",
    "include/gssrpc/auth.h",
    "include/gssrpc/auth_gss.h",
    "include/gssrpc/auth_gssapi.h",
    "include/gssrpc/auth_unix.h",
    "include/gssrpc/clnt.h",
    "include/gssrpc/netdb.h",
    "include/gssrpc/pmap_clnt.h",
    "include/gssrpc/pmap_prot.h",
    "include/gssrpc/pmap_rmt.h",
    "include/gssrpc/rename.h",
    "include/gssrpc/rpc.h",
    "include/gssrpc/rpc_msg.h",
    "include/gssrpc/svc.h",
    "include/gssrpc/svc_auth.h",
    "include/gssrpc/types.h",
    "include/gssrpc/xdr.h",
]

lib_files = [
    "lib/libcom_err.a",
    "lib/libgssapi_krb5.a",
    "lib/libgssrpc.a",
    "lib/libk5crypto.a",
    "lib/libkadm5clnt.a",
    "lib/libkadm5clnt_mit.a",
    "lib/libkadm5srv.a",
    "lib/libkadm5srv_mit.a",
    "lib/libkdb5.a",
    "lib/libkrad.a",
    "lib/libkrb5.a",
    "lib/libkrb5_db2.a",
    "lib/libkrb5_k5tls.a",
    "lib/libkrb5_otp.a",
    "lib/libkrb5_pkinit.a",
    "lib/libkrb5_test.a",
    "lib/libkrb5support.a",
    "lib/libverto.a",
]

genrule(
    name = "krb5-srcs",
    srcs = [
        package_file,
        "//third_party/openssl:openssl-files",
    ],
    outs = include_files + lib_files, 
    cmd = "\n".join([
        'export OPENSSL_DIR=$$(pwd)/$(GENDIR)/third_party/openssl',
        'export INSTALL_DIR=$$(pwd)/$(@D)',
        'export TMP_DIR=$$(mktemp -d -t krb5.XXXXX)',
        'mkdir -p $$TMP_DIR',
        'cp -R $(SRCS) $$TMP_DIR',
        'cd $$TMP_DIR',
        'tar xfz ' + package_file,
        'cd ' + package_dir,
        'cd src',
        './configure --prefix=$$INSTALL_DIR CPPFLAGS="-I$$OPENSSL_DIR/include" LDFLAGS="-L$$OPENSSL_DIR/lib -lcrypto -lssl" --enable-static --enable-shared=no',
        'make',
        'make install',
        'rm -rf $$TMP_DIR',
    ]),
)

cc_library(
    name = "krb5-cxx",
    srcs = ["empty.cc"] + lib_files,
    deps = [
      "//third_party/openssl:openssl-cxx",
    ],
    hdrs = include_files,
    includes = [
        "include",
    ],
    linkstatic = 1,
)

filegroup(
    name = "krb5",
    srcs = [
        ":krb5-cxx",
    ]
)
