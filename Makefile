# ===============================
# Config
# ===============================
DEV_COMPOSE=docker-compose.dev.yml
PROD_COMPOSE=docker-compose.prd.yml
DEV_BUILD_SERVICES=airflow-webserver dbt jupyter
PROD_BUILD_SERVICES=airflow-webserver dbt
DBT_PROJECT_DIR=/usr/app/synthetic_pricing_pipeline
DBT_PROFILE_NAME=synthetic_pricing_platform
ENV_FILE=env/synthetic_pipeline.env
GCLOUD_PATH?=$(HOME)/.config/gcloud

# ===============================
# DEV
# ===============================
.PHONY: dev-up dev-down dev-restart dev-build dev-logs dev-ps dev-init dev-reset

dev-up:
	GCLOUD_PATH=$(GCLOUD_PATH) docker compose -f $(DEV_COMPOSE) build $(DEV_BUILD_SERVICES)
	GCLOUD_PATH=$(GCLOUD_PATH) docker compose -f $(DEV_COMPOSE) up -d --no-build

dev-down:
	GCLOUD_PATH=$(GCLOUD_PATH) docker compose -f $(DEV_COMPOSE) down

dev-restart:
	GCLOUD_PATH=$(GCLOUD_PATH) docker compose -f $(DEV_COMPOSE) down
	GCLOUD_PATH=$(GCLOUD_PATH) docker compose -f $(DEV_COMPOSE) build $(DEV_BUILD_SERVICES)
	GCLOUD_PATH=$(GCLOUD_PATH) docker compose -f $(DEV_COMPOSE) up -d --no-build

dev-build:
	GCLOUD_PATH=$(GCLOUD_PATH) docker compose -f $(DEV_COMPOSE) build $(DEV_BUILD_SERVICES)
	GCLOUD_PATH=$(GCLOUD_PATH) docker compose -f $(DEV_COMPOSE) up -d --no-build

dev-ps:
	GCLOUD_PATH=$(GCLOUD_PATH) docker compose -f $(DEV_COMPOSE) ps

dev-logs:
	GCLOUD_PATH=$(GCLOUD_PATH) docker compose -f $(DEV_COMPOSE) logs -f

# init ครั้งแรกของ dev (ทำครั้งเดียว)
dev-init:
	GCLOUD_PATH=$(GCLOUD_PATH) docker compose -f $(DEV_COMPOSE) build $(DEV_BUILD_SERVICES)
	GCLOUD_PATH=$(GCLOUD_PATH) docker compose -f $(DEV_COMPOSE) up -d --no-build
	docker exec -it dbt dbt init $(DBT_PROFILE_NAME)
	docker exec -it great-expectations great_expectations init

# reset dev ทั้งหมด (ลบ volume)
dev-reset:
	GCLOUD_PATH=$(GCLOUD_PATH) docker compose -f $(DEV_COMPOSE) down -v
	GCLOUD_PATH=$(GCLOUD_PATH) docker compose -f $(DEV_COMPOSE) build $(DEV_BUILD_SERVICES)
	GCLOUD_PATH=$(GCLOUD_PATH) docker compose -f $(DEV_COMPOSE) up -d --no-build

# ===============================
# DEV - dbt / GE
# ===============================
dev-dbt-debug:
	docker exec -it -w $(DBT_PROJECT_DIR) dbt dbt debug

dev-dbt-run:
	docker exec -it -w $(DBT_PROJECT_DIR) dbt dbt run

dev-dbt-test:
	docker exec -it -w $(DBT_PROJECT_DIR) dbt dbt test

dev-ge-check:
	docker exec -it great-expectations great_expectations checkpoint run all

# ===============================
# PROD (ระวัง!)
# ===============================
.PHONY: prod-build prod-up prod-down prod-ps prod-logs

prod-build:
	GCLOUD_PATH=$(GCLOUD_PATH) docker compose --env-file $(ENV_FILE) -f $(PROD_COMPOSE) build $(PROD_BUILD_SERVICES)

prod-up:
	GCLOUD_PATH=$(GCLOUD_PATH) docker compose --env-file $(ENV_FILE) -f $(PROD_COMPOSE) build $(PROD_BUILD_SERVICES)
	GCLOUD_PATH=$(GCLOUD_PATH) docker compose --env-file $(ENV_FILE) -f $(PROD_COMPOSE) up -d --no-build

prod-down:
	GCLOUD_PATH=$(GCLOUD_PATH) docker compose --env-file $(ENV_FILE) -f $(PROD_COMPOSE) down

prod-ps:
	GCLOUD_PATH=$(GCLOUD_PATH) docker compose --env-file $(ENV_FILE) -f $(PROD_COMPOSE) ps

prod-logs:
	GCLOUD_PATH=$(GCLOUD_PATH) docker compose --env-file $(ENV_FILE) -f $(PROD_COMPOSE) logs -f
