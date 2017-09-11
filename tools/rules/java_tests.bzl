def java_tests(test_classes, runtime_deps=[], resources=[], data=[], size="medium"):
    for test_class in test_classes:
        native.java_test(
            name = test_class.split(".")[-1],
            runtime_deps = runtime_deps,
            size = size,
            test_class = test_class,
            resources = resources,
            data = data,
        )