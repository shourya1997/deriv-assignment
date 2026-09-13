def test_package_importable():
    import deriv_pipeline  # noqa: F401
    import deriv_pipeline.db  # noqa: F401
    import deriv_pipeline.migrate  # noqa: F401
