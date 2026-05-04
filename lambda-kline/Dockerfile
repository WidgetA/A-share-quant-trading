FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt ${LAMBDA_TASK_ROOT}
# --only-binary=:all: refuses to compile from sdist. Lambda base images have
# no C/C++ toolchain — if a wheel goes missing for our platform, fail loudly
# at build time instead of falling into a slow meson build that exits with a
# cryptic "Unknown compiler(s)" error.
RUN pip install --no-cache-dir --only-binary=:all: -r requirements.txt

COPY app.py ${LAMBDA_TASK_ROOT}

CMD ["app.lambda_handler"]
