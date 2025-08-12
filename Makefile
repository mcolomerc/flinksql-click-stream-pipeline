# Makefile for Click Stream Pipeline

VENV_NAME := venv
PYTHON := python3

.PHONY: help setup install run clean all-in-one venv-clean

help:		## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

all-in-one:	## Create venv, install deps, setup env, and run pipeline
	@echo "🚀 Starting complete pipeline setup and execution..."
	@echo "📦 Creating Python virtual environment..."
	$(PYTHON) -m venv $(VENV_NAME)
	@echo "📥 Installing dependencies..."
	./$(VENV_NAME)/bin/pip install --upgrade pip
	./$(VENV_NAME)/bin/pip install -r requirements.txt
	@echo "⚙️  Setting up environment..."
	./$(VENV_NAME)/bin/python setup.py || echo "⚠️  Please configure .env file manually"
	@echo "🎯 Running complete pipeline..."
	./$(VENV_NAME)/bin/python run_pipeline.py

setup:		## Setup environment and install dependencies
	python setup.py

install:	## Install Python dependencies only
	pip install -r requirements.txt

run:		## Run the complete pipeline
	python run_pipeline.py

clean:		## Clean up all pipeline resources
	python cleanup.py

topics:		## Create Kafka topics only
	python topics.py

producer:	## Run producer only (generate events)
	python producer.py

consumer:	## Run consumer only (read outputs)
	python consumer.py

flink:		## Deploy Flink SQL pipeline only
	python flink_sql.py

config:		## Validate configuration
	python -c "from config import config; config.validate_config()"

venv-clean:	## Remove virtual environment
	@echo "🗑️  Removing virtual environment..."
	rm -rf $(VENV_NAME)

# Default target
.DEFAULT_GOAL := help
