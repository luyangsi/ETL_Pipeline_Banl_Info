FROM python:3.12-slim

# System deps for lxml (pandas read_html relies on lxml)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libxml2-dev \
    libxslt1-dev \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy minimal files first for better docker caching
COPY pyproject.toml /app/pyproject.toml
COPY src /app/src
COPY data /app/data
COPY README.md /app/README.md

RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -e .

# Default command (can be overridden)
CMD ["largest-banks-etl", \
  "--url", "https://en.wikipedia.org/wiki/List_of_largest_banks", \
  "--rates", "data/exchange_rates.csv", \
  "--out-csv", "outputs/largest_banks.csv", \
  "--db", "outputs/largest_banks.db", \
  "--table", "Largest_banks", \
  "--log", "etl_project_log.txt" \
]
