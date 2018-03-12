licenses(["notice"])

package(default_visibility = ["//visibility:public"])

generated_files = [
    "ParseException.java",
    "Token.java",
    "TokenMgrError.java",
    "SimpleCharStream.java",
    "HeronSqlParserImpl.java",
    "HeronSqlParserImplConstants.java",
    "HeronSqlParserImplTokenManager.java",
]

genrule(
    name = "parser-srcs",
    srcs = ["com/twitter/heron/sql/parser/impl/Parser.jj"],
    outs = generated_files, 
    tools = [
        "//third_party/java:javacc",
    ],
    cmd = "\n".join([
        'export INSTALL_DIR=$$(pwd)/$(@D)',
        '$(location //third_party/java:javacc) -OUTPUT_DIRECTORY=$$INSTALL_DIR $<',
    ]),
)
