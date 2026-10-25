.PHONY: setup mock run rag reset clean help

help:
	@echo "Available commands:"
	@echo "  make setup   - Install dependencies and start Docker PostgreSQL"
	@echo "  make mock    - Start the Progress OpenEdge mock server"
	@echo "  make run     - Launch the Master Orchestrator Pipeline"
	@echo "  make rag     - Launch the Agentic RAG Knowledge Layer"
	@echo "  make reset   - Reset environment (clear DuckDB & PostgreSQL schemas)"
	@echo "  make clean   - Stop Docker containers and clean local state"

setup:
	pip install --upgrade pip
	pip install -r requirements.txt
	docker-compose up -d --no-recreate postgres_db

mock:
	python src/mock_openedge_server.py

run:
	python -u src/pipeline.py

rag:
	python src/agent_rag.py

reset:
	python src/reset.py

clean:
	docker-compose down -v
	rm -f data_engine.duckdb pipeline_execution.log