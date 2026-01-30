# DataOps – Customers pipeline

## Prerequisites
- Docker
- (Optional) Python 3.11 + venv for local tests

## Project structure
- Input CSV: `data/raw/`
- Output CSV: `data/clean/`
- Pipeline code: `main/pipelines.py`
- Tests: `main/tests/`

## Run the pipeline with Docker (no local Python needed)
Build the image:
```bash
docker build -t customers-pipeline:latest .