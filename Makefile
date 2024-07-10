install:
	pip install --upgrade pip &&\
	pip install -r requirements.txt

format:
	black *.py mylib/*.py

lint:
	pylint --disable=R,C *.py mylib/*.py tasks/*.py

test:
	python -m pytest -vv --cov=mylib --cov=tasks --cov=main  test_*.py

build:
	docker build . -t myapp

up:
	docker run myapp

down:
	docker stop myapp

all: install lint test build
