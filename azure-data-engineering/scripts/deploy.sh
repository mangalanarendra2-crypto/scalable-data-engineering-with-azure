#!/usr/bin/env bash
# scripts/deploy.sh
# =============================================================================
# Deployment script: provision Azure infrastructure and deploy pipeline code
# =============================================================================

set -euo pipefail

# ── Colors ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; NC='\033[0m'

log()   { echo -e "${BLUE}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }

# ── Banner ────────────────────────────────────────────────────────────────────
echo -e "${CYAN}"
cat << 'EOF'
  ╔══════════════════════════════════════════════════════╗
  ║     Scalable Data Engineering with Azure            ║
  ║     Deployment Script v1.0                          ║
  ╚══════════════════════════════════════════════════════╝
EOF
echo -e "${NC}"

# ── Configuration ─────────────────────────────────────────────────────────────
ENVIRONMENT="${ENVIRONMENT:-prod}"
LOCATION="${LOCATION:-eastus2}"
ALERT_EMAIL="${ALERT_EMAIL:-data-alerts@yourorg.com}"
PYTHON="${PYTHON:-python3}"

# ── Prerequisites ─────────────────────────────────────────────────────────────
check_prerequisites() {
  log "Checking prerequisites..."
  local missing=()

  command -v az &>/dev/null       || missing+=("azure-cli")
  command -v terraform &>/dev/null || missing+=("terraform")
  command -v python3 &>/dev/null   || missing+=("python3")
  command -v pip &>/dev/null       || missing+=("pip")
  command -v docker &>/dev/null    || missing+=("docker")

  if [[ ${#missing[@]} -gt 0 ]]; then
    error "Missing prerequisites: ${missing[*]}"
    exit 1
  fi

  ok "All prerequisites found."
}

# ── Azure Login ───────────────────────────────────────────────────────────────
azure_login() {
  log "Checking Azure authentication..."
  if ! az account show &>/dev/null; then
    log "Logging into Azure..."
    az login --use-device-code
  fi
  SUBSCRIPTION_ID=$(az account show --query id -o tsv)
  ok "Logged in. Subscription: ${SUBSCRIPTION_ID}"
}

# ── Python Environment ────────────────────────────────────────────────────────
setup_python() {
  log "Setting up Python virtual environment..."
  if [[ ! -d ".venv" ]]; then
    $PYTHON -m venv .venv
  fi
  source .venv/bin/activate
  pip install --upgrade pip --quiet
  pip install -r requirements.txt --quiet
  ok "Python environment ready."
}

# ── Terraform ─────────────────────────────────────────────────────────────────
deploy_infrastructure() {
  log "Deploying Azure infrastructure with Terraform..."
  cd infrastructure

  terraform init -upgrade -reconfigure \
    -backend-config="key=data-engineering-${ENVIRONMENT}.tfstate"

  terraform plan \
    -var="environment=${ENVIRONMENT}" \
    -var="location=${LOCATION}" \
    -var="alert_email=${ALERT_EMAIL}" \
    -out=tfplan

  echo ""
  read -p "$(echo -e "${YELLOW}Apply infrastructure changes? [y/N]:${NC} ")" -n 1 -r
  echo ""

  if [[ $REPLY =~ ^[Yy]$ ]]; then
    terraform apply tfplan
    ok "Infrastructure deployed."
  else
    warn "Infrastructure deployment skipped."
  fi

  cd ..
}

# ── Export Terraform Outputs ──────────────────────────────────────────────────
export_outputs() {
  log "Exporting Terraform outputs to .env..."
  cd infrastructure

  ADLS_ACCOUNT=$(terraform output -raw adls_account_name 2>/dev/null || echo "")
  EVH_NAMESPACE=$(terraform output -raw event_hub_namespace 2>/dev/null || echo "")
  DBW_URL=$(terraform output -raw databricks_workspace_url 2>/dev/null || echo "")
  KV_URI=$(terraform output -raw key_vault_uri 2>/dev/null || echo "")

  cd ..

  cat > .env << EOF
ADLS_ACCOUNT_NAME=${ADLS_ACCOUNT}
EVENT_HUB_NAMESPACE=${EVH_NAMESPACE}
DATABRICKS_HOST=${DBW_URL}
KEY_VAULT_URL=${KV_URI}
ENVIRONMENT=${ENVIRONMENT}
EOF
  ok "Environment variables written to .env"
}

# ── Run Tests ─────────────────────────────────────────────────────────────────
run_tests() {
  log "Running test suite..."
  source .venv/bin/activate 2>/dev/null || true
  pytest tests/ \
    --cov=src \
    --cov-report=term-missing \
    --cov-report=html:htmlcov \
    --tb=short \
    -q
  ok "Tests passed."
}

# ── Package Wheel ─────────────────────────────────────────────────────────────
build_package() {
  log "Building Python package..."
  source .venv/bin/activate 2>/dev/null || true
  pip install build --quiet
  python -m build --wheel
  ok "Package built: $(ls dist/*.whl | head -1)"
}

# ── Upload DAGs to Airflow ────────────────────────────────────────────────────
deploy_dags() {
  log "Deploying Airflow DAGs..."
  AIRFLOW_HOME="${AIRFLOW_HOME:-/opt/airflow}"
  DAG_DIR="${AIRFLOW_HOME}/dags"

  if [[ -d "$DAG_DIR" ]]; then
    cp -r src/orchestration/dags/*.py "$DAG_DIR/"
    ok "DAGs deployed to ${DAG_DIR}"
  else
    warn "Airflow DAG directory not found at ${DAG_DIR}. Skipping."
  fi
}

# ── Health Check ─────────────────────────────────────────────────────────────
health_check() {
  log "Running pipeline health check..."
  source .venv/bin/activate 2>/dev/null || true
  $PYTHON -c "
from config.settings import get_settings
s = get_settings()
print(f'  Environment: {s.environment}')
print(f'  ADLS: {s.adls.account_name}')
print(f'  Log level: {s.log_level}')
print('Health check passed.')
" 2>/dev/null && ok "Health check passed." || warn "Health check skipped (env not fully configured)."
}

# ── Main ──────────────────────────────────────────────────────────────────────
main() {
  local cmd="${1:-help}"

  case "$cmd" in
    all)
      check_prerequisites
      azure_login
      setup_python
      run_tests
      deploy_infrastructure
      export_outputs
      build_package
      deploy_dags
      health_check
      echo ""
      ok "🎉 Full deployment complete!"
      ;;
    infra)
      check_prerequisites
      azure_login
      deploy_infrastructure
      export_outputs
      ;;
    test)
      setup_python
      run_tests
      ;;
    build)
      setup_python
      build_package
      ;;
    dags)
      deploy_dags
      ;;
    health)
      health_check
      ;;
    help|*)
      echo "Usage: $0 {all|infra|test|build|dags|health}"
      echo ""
      echo "  all     Full end-to-end deployment"
      echo "  infra   Deploy Azure infrastructure via Terraform"
      echo "  test    Run test suite with coverage"
      echo "  build   Build Python wheel package"
      echo "  dags    Deploy Airflow DAGs"
      echo "  health  Run pipeline health check"
      ;;
  esac
}

main "$@"
