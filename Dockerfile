FROM public.ecr.aws/lambda/python:3.13

COPY requirements-serverless.txt ${LAMBDA_TASK_ROOT}/requirements-serverless.txt
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements-serverless.txt

COPY app ${LAMBDA_TASK_ROOT}/app
COPY assets/icon.png ${LAMBDA_TASK_ROOT}/assets/icon.png
COPY config ${LAMBDA_TASK_ROOT}/config
COPY schemas ${LAMBDA_TASK_ROOT}/schemas

CMD ["app.lambda_handlers.web_handler"]
