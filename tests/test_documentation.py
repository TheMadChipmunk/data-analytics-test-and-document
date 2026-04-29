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


def _has_description(models_list, model_name):
    """Return True if a named model in the list has a non-empty description."""
    for model in models_list:
        if model.get("name") == model_name:
            desc = model.get("description", "")
            return bool(desc and str(desc).strip())
    return False


def _columns_have_descriptions(models_list, model_name):
    """Return True if ALL columns in the named model have non-empty descriptions."""
    for model in models_list:
        if model.get("name") == model_name:
            columns = model.get("columns", [])
            if not columns:
                return False
            return all(
                bool(col.get("description", "").strip())
                for col in columns
            )
    return False


class TestDocumentation:
    """Checkpoint 3 — model and column descriptions added across all layers."""

    # ── Staging ──────────────────────────────────────────────────────────────

    def test_staging_schema_exists(self, project_dir):
        schema_path = project_dir / "models" / "staging" / "schema.yml"
        assert schema_path.exists(), (
            "❌ models/staging/schema.yml not found\n"
            "   Did you complete Checkpoint 2 first?"
        )

    def test_staging_models_have_descriptions(self, project_dir):
        schema_path = project_dir / "models" / "staging" / "schema.yml"
        if not schema_path.exists():
            pytest.skip("staging/schema.yml not found")
        with open(schema_path) as f:
            content = yaml.safe_load(f)
        models = content.get("models", []) if content else []
        for model_name in ["stg_customers", "stg_orders", "stg_payments"]:
            assert _has_description(models, model_name), (
                f"❌ {model_name} is missing a model-level description in models/staging/schema.yml\n"
                f"   Add a 'description:' key under the {model_name} entry."
            )

    def test_staging_columns_have_descriptions(self, project_dir):
        schema_path = project_dir / "models" / "staging" / "schema.yml"
        if not schema_path.exists():
            pytest.skip("staging/schema.yml not found")
        with open(schema_path) as f:
            content = yaml.safe_load(f)
        models = content.get("models", []) if content else []
        for model_name in ["stg_customers", "stg_orders", "stg_payments"]:
            assert _columns_have_descriptions(models, model_name), (
                f"❌ Not all columns in {model_name} have descriptions in models/staging/schema.yml\n"
                "   Add a 'description:' to every column entry."
            )

    # ── Intermediate ─────────────────────────────────────────────────────────

    def test_intermediate_schema_exists(self, project_dir):
        schema_path = project_dir / "models" / "intermediate" / "schema.yml"
        assert schema_path.exists(), (
            "❌ models/intermediate/schema.yml not found\n"
            "   Did you complete Checkpoint 1 first?"
        )

    def test_intermediate_model_has_description(self, project_dir):
        schema_path = project_dir / "models" / "intermediate" / "schema.yml"
        if not schema_path.exists():
            pytest.skip("intermediate/schema.yml not found")
        with open(schema_path) as f:
            content = yaml.safe_load(f)
        models = content.get("models", []) if content else []
        for model_name in ["int_orders_with_payments", "int_customer_order_summary"]:
            assert _has_description(models, model_name), (
                f"❌ {model_name} is missing a model-level description\n"
                f"   Add a 'description:' key under {model_name} in models/intermediate/schema.yml."
            )

    def test_intermediate_columns_have_descriptions(self, project_dir):
        schema_path = project_dir / "models" / "intermediate" / "schema.yml"
        if not schema_path.exists():
            pytest.skip("intermediate/schema.yml not found")
        with open(schema_path) as f:
            content = yaml.safe_load(f)
        models = content.get("models", []) if content else []
        for model_name in ["int_orders_with_payments", "int_customer_order_summary"]:
            assert _columns_have_descriptions(models, model_name), (
                f"❌ Not all columns in {model_name} have descriptions\n"
                "   Add a 'description:' to every column entry in models/intermediate/schema.yml."
            )

    # ── Marts ─────────────────────────────────────────────────────────────────

    def test_marts_schema_exists(self, project_dir):
        schema_path = project_dir / "models" / "marts" / "schema.yml"
        assert schema_path.exists(), (
            "❌ models/marts/schema.yml not found\n"
            "   Did you complete Checkpoint 1 first?"
        )

    def test_marts_models_have_descriptions(self, project_dir):
        schema_path = project_dir / "models" / "marts" / "schema.yml"
        if not schema_path.exists():
            pytest.skip("marts/schema.yml not found")
        with open(schema_path) as f:
            content = yaml.safe_load(f)
        models = content.get("models", []) if content else []
        for model_name in ["dim_customers", "fct_orders"]:
            assert _has_description(models, model_name), (
                f"❌ {model_name} is missing a model-level description in models/marts/schema.yml\n"
                f"   Add a 'description:' key under the {model_name} entry."
            )

    def test_marts_columns_have_descriptions(self, project_dir):
        schema_path = project_dir / "models" / "marts" / "schema.yml"
        if not schema_path.exists():
            pytest.skip("marts/schema.yml not found")
        with open(schema_path) as f:
            content = yaml.safe_load(f)
        models = content.get("models", []) if content else []
        for model_name in ["dim_customers", "fct_orders"]:
            assert _columns_have_descriptions(models, model_name), (
                f"❌ Not all columns in {model_name} have descriptions in models/marts/schema.yml\n"
                "   Add a 'description:' to every column entry."
            )
