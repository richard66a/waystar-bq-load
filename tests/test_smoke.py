def test_import_test_pipeline():
    import importlib
    mod = importlib.import_module('tests.test_pipeline')
    assert hasattr(mod, 'run_all_tests') and callable(mod.run_all_tests)

def test_validator_script_exists():
    import os
    assert os.path.exists(os.path.join(os.path.dirname(__file__), 'validate_local_samples.py'))
