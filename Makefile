default:  ## Runs the django application
	. ./venv/bin/activate && ./main.py

env: ## Install the dependencies
	. ./venv/bin/activate && pip install -r requirements.txt

freeze: ## Create the requirements.txt from the environment
	. ./venv/bin/activate && pip freeze > requirements.txt


lint: ## Lints the code in the application
	@./venv/bin/black ./*.py
	@./venv/bin/black ./**/*.py

help: ## Displays the help
	@cat $(MAKEFILE_LIST) | grep -E '^[a-zA-Z_-]+:.*?## .*$$' | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
