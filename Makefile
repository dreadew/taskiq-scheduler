.PHONY: gen-proto start-taskiq-worker start-fastapi upgrade downgrade history

gen-proto:
	@mkdir -p src/generated
	@touch src/generated/__init__.py
	poetry run python -m grpc_tools.protoc \
		-I proto \
		--python_out=. \
		--grpc_python_out=. \
		proto/src/generated/schema_review.proto

start-taskiq-worker:
	taskiq worker src.infra.brokers.worker:nats_broker --workers=4

start-fastapi:
	uvicorn src.api.app:app --host 0.0.0.0 --port 8080 --reload

upgrade:
	alembic upgrade head

downgrade:
	alembic downgrade 1

history:
	alembic history --verbose