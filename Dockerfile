# Build the single-page workspace. The official Node image is pulled from the ECR
# Public mirror to avoid Docker Hub rate limits in CI.
FROM public.ecr.aws/docker/library/node:24-alpine AS frontend
WORKDIR /frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY frontend/ ./
RUN npm run build

FROM public.ecr.aws/lambda/python:3.13

COPY requirements-serverless.txt ${LAMBDA_TASK_ROOT}/requirements-serverless.txt
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements-serverless.txt

COPY app ${LAMBDA_TASK_ROOT}/app
COPY assets/icon.png ${LAMBDA_TASK_ROOT}/assets/icon.png
COPY config ${LAMBDA_TASK_ROOT}/config
COPY schemas ${LAMBDA_TASK_ROOT}/schemas
COPY --from=frontend /frontend/dist ${LAMBDA_TASK_ROOT}/frontend/dist

CMD ["app.lambda_handlers.web_handler"]
