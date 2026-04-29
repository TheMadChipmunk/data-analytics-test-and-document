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


class TestMartTests:
    """Checkpoint 1 — data quality tests added to mart and intermediate models."""

    def test_intermediate_schema_exists(self, project_dir):
        """models/intermediate/schema.yml must exist."""
        schema_path = project_dir / "models" / "intermediate" / "schema.yml"
        assert schema_path.exists(), (
            "❌ models/intermediate/schema.yml not found\n"
            "   Did you create the intermediate schema file with tests?"
        )

    def test_marts_schema_exists(self, project_dir):
        """models/marts/schema.yml must exist."""
        schema_path = project_dir / "models" / "marts" / "schema.yml"
        assert schema_path.exists(), (
            "❌ models/marts/schema.yml not found\n"
            "   Did you create the marts schema file with tests?"
        )

    def test_intermediate_schema_has_models(self, project_dir):
        """models/intermediate/schema.yml must contain a models: section."""
        schema_path = project_dir / "models" / "intermediate" / "schema.yml"
        if not schema_path.exists():
            pytest.skip("intermediate/schema.yml not found")
        with open(schema_path) as f:
            content = yaml.safe_load(f)
        assert content is not None and "models" in content, (
            "❌ models/intermediate/schema.yml has no 'models:' section\n"
            "   Did you add int_orders_with_payments documentation and tests?"
        )

    def test_marts_schema_has_models(self, project_dir):
        """models/marts/schema.yml must contain a models: section."""
        schema_path = project_dir / "models" / "marts" / "schema.yml"
        if not schema_path.exists():
            pytest.skip("marts/schema.yml not found")
        with open(schema_path) as f:
            content = yaml.safe_load(f)
        assert content is not None and "models" in content, (
            "❌ models/marts/schema.yml has no 'models:' section\n"
            "   Did you add dim_customers and fct_orders documentation and tests?"
        )

    def test_intermediate_has_tests(self, project_dir):
        """int_orders_with_payments must have tests defined."""
        schema_path = project_dir / "models" / "intermediate" / "schema.yml"
        if not schema_path.exists():
            pytest.skip("intermediate/schema.yml not found")
        with open(schema_path) as f:
            raw = f.read()
        assert "tests:" in raw, (
            "❌ No tests: found in models/intermediate/schema.yml\n"
            "   Did you add unique, not_null, or accepted_values tests to int_orders_with_payments?"
        )

    def test_marts_has_tests(self, project_dir):
        """dim_customers or fct_orders must have tests defined."""
        schema_path = project_dir / "models" / "marts" / "schema.yml"
        if not schema_path.exists():
            pytest.skip("marts/schema.yml not found")
        with open(schema_path) as f:
            raw = f.read()
        assert "tests:" in raw, (
            "❌ No tests: found in models/marts/schema.yml\n"
            "   Did you add unique, not_null, or relationships tests to dim_customers and fct_orders?"
        )

    def test_fct_orders_relationships_test(self, project_dir):
        """fct_orders must include a relationships test."""
        schema_path = project_dir / "models" / "marts" / "schema.yml"
        if not schema_path.exists():
            pytest.skip("marts/schema.yml not found")
        with open(schema_path) as f:
            raw = f.read()
        assert "relationships:" in raw, (
            "❌ No relationships: test found in models/marts/schema.yml\n"
            "   Did you add a relationships test on fct_orders.customer_id → dim_customers.customer_id?"
        )

    def test_intermediate_accepted_values_test(self, project_dir):
        """int_orders_with_payments.status must have accepted_values test."""
        schema_path = project_dir / "models" / "intermediate" / "schema.yml"
        if not schema_path.exists():
            pytest.skip("intermediate/schema.yml not found")
        with open(schema_path) as f:
            raw = f.read()
        assert "accepted_values:" in raw, (
            "❌ No accepted_values: test found in models/intermediate/schema.yml\n"
            "   Did you add an accepted_values test on int_orders_with_payments.status?"
        )
