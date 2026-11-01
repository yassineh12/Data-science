# Production image for a service or pipeline built on dsops.
# Build from your project root:  docker build -f Dockerfile -t my-pipeline .
FROM python:3.12-slim AS build
WORKDIR /src
COPY . .
RUN pip install --no-cache-dir --upgrade pip build \
 && python -m build --wheel --outdir /wheels

FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
RUN useradd --create-home --uid 10001 app
COPY --from=build /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl && rm -rf /wheels
USER app
WORKDIR /home/app
EXPOSE 9100
HEALTHCHECK --interval=30s --timeout=3s CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:9100/healthz').status==200 else 1)"
# Replace with your entrypoint, e.g. a DAG:  dsops --log-format json dag run my_pkg.pipelines:dag
CMD ["dsops", "--log-format", "json", "serve", "--port", "9100"]
