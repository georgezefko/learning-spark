# The name of the Docker Compose file
COMPOSE_FILE = docker-compose.yml


#Run Spark Configutation tool
spark_config:
	cd src/spark_configuration
	streamlit run main.py

# Run the containers in the background
up:
	docker-compose -f $(COMPOSE_FILE) up -d

# Stop the containers
down:
	docker-compose -f $(COMPOSE_FILE) down

# Rebuild the Docker image and restart the containers
rebuild: build down up

# Show logs
logs:
	docker-compose -f $(COMPOSE_FILE) logs

# Open the browser
browse:
	open http://localhost:4040

unit-tests:
	@echo "Running unit tests..."
	cd mage && python -m pytest tests/unit
