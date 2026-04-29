import pytest
from pathlib import Path
import yaml


@pytest.fixture
def project_dir():
    """Get jaffle_shop_dbt directory within challenge repo."""
    challenge_dir = Path(__file__).parent.parent
    dbt_project_dir = challenge_dir / "jaffle_shop_dbt"

    assert dbt_project_dir.exists(), (
        f"❌ jaffle_shop_dbt/ directory not found in {challenge_dir}\n"
        f"   Did you copy your dbt project from the previous challenge? (Section 0)\n"
        f"   Run: cp -rP ../PREVIOUS-CHALLENGE/jaffle_shop_dbt ."
    )
    return dbt_project_dir


class TestStagingTests:
    """Checkpoint 2 — data quality tests added to staging models in their own schema file."""

    def test_staging_schema_exists(self, project_dir):
        """models/staging/schema.yml must exist."""
        schema_path = project_dir / "models" / "staging" / "schema.yml"
        assert schema_path.exists(), (
            "❌ models/staging/schema.yml not found\n"
            "   Did you create a separate schema file for your staging models?"
        )

    def test_staging_schema_has_models(self, project_dir):
        """models/staging/schema.yml must have a models: section."""
        schema_path = project_dir / "models" / "staging" / "schema.yml"
        if not schema_path.exists():
            pytest.skip("staging/schema.yml not found")
        with open(schema_path) as f:
            content = yaml.safe_load(f)
        assert content is not None and "models" in content, (
            "❌ models/staging/schema.yml has no 'models:' section\n"
            "   Did you add stg_customers, stg_orders, and stg_payments entries?"
        )

    def test_staging_schema_has_three_models(self, project_dir):
        """models/staging/schema.yml must document at least 3 staging models."""
        schema_path = project_dir / "models" / "staging" / "schema.yml"
        if not schema_path.exists():
            pytest.skip("staging/schema.yml not found")
        with open(schema_path) as f:
            content = yaml.safe_load(f)
        if not content or "models" not in content:
            pytest.skip("no models section")
        model_names = [m.get("name", "") for m in content["models"]]
        assert len(model_names) >= 3, (
            f"❌ Only {len(model_names)} models found in models/staging/schema.yml\n"
            "   Did you add stg_customers, stg_orders, AND stg_payments?"
        )

    def test_staging_schema_has_tests(self, project_dir):
        """models/staging/schema.yml must contain at least one tests: block."""
        schema_path = project_dir / "models" / "staging" / "schema.yml"
        if not schema_path.exists():
            pytest.skip("staging/schema.yml not found")
        with open(schema_path) as f:
            raw = f.read()
        assert "tests:" in raw, (
            "❌ No tests: found in models/staging/schema.yml\n"
            "   Did you add unique and not_null tests to your staging model primary keys?"
        )

    def test_staging_schema_has_unique_test(self, project_dir):
        """staging/schema.yml must include at least one unique test."""
        schema_path = project_dir / "models" / "staging" / "schema.yml"
        if not schema_path.exists():
            pytest.skip("staging/schema.yml not found")
        with open(schema_path) as f:
            raw = f.read()
        assert "unique" in raw, (
            "❌ No 'unique' test found in models/staging/schema.yml\n"
            "   Did you add a unique test on customer_id, order_id, and payment_id?"
        )

    def test_staging_schema_has_not_null_test(self, project_dir):
        """staging/schema.yml must include at least one not_null test."""
        schema_path = project_dir / "models" / "staging" / "schema.yml"
        if not schema_path.exists():
            pytest.skip("staging/schema.yml not found")
        with open(schema_path) as f:
            raw = f.read()
        assert "not_null" in raw, (
            "❌ No 'not_null' test found in models/staging/schema.yml\n"
            "   Did you add not_null tests to your primary key columns?"
        )

    def test_staging_schema_has_accepted_values(self, project_dir):
        """staging/schema.yml must include an accepted_values test on payment_method."""
        schema_path = project_dir / "models" / "staging" / "schema.yml"
        if not schema_path.exists():
            pytest.skip("staging/schema.yml not found")
        with open(schema_path) as f:
            raw = f.read()
        assert "accepted_values:" in raw, (
            "❌ No accepted_values: test found in models/staging/schema.yml\n"
            "   Did you add an accepted_values test on stg_payments.payment_method?"
        )

    def test_root_schema_not_duplicating_staging_models(self, project_dir):
        """models/schema.yml should not duplicate staging model definitions."""
        root_schema = project_dir / "models" / "schema.yml"
        staging_schema = project_dir / "models" / "staging" / "schema.yml"
        if not root_schema.exists() or not staging_schema.exists():
            pytest.skip("one or both schema files not found")
        with open(root_schema) as f:
            root_content = yaml.safe_load(f)
        with open(staging_schema) as f:
            staging_content = yaml.safe_load(f)

        root_models = set()
        if root_content and "models" in root_content:
            root_models = {m.get("name", "") for m in root_content["models"]}

        staging_models = set()
        if staging_content and "models" in staging_content:
            staging_models = {m.get("name", "") for m in staging_content["models"]}

        duplicates = root_models & staging_models
        assert not duplicates, (
            f"❌ Models defined in both models/schema.yml and models/staging/schema.yml: {duplicates}\n"
            "   Remove staging models from models/schema.yml — it should contain only sources: now."
        )
