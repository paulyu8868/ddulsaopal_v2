# Lambda 컨테이너 이미지 — 의존성 + 래퍼만 포함 (매매 코드는 런타임에 main을 clone)
# 빌드 컨텍스트는 레포 루트: docker build -f aws/Dockerfile .
FROM public.ecr.aws/lambda/python:3.10

RUN yum install -y git && yum clean all

COPY requirements.txt .
# matplotlib/plotly는 차트 스크립트(이관 제외) 전용 — Lambda 이미지에서 제외
# (pillow가 AL2 glibc 2.26용 휠을 제공하지 않아 소스 빌드로 실패하는 문제도 함께 회피)
RUN pip install --no-cache-dir --upgrade pip && \
    grep -vE "^(matplotlib|plotly)" requirements.txt > /tmp/requirements-lambda.txt && \
    pip install --no-cache-dir -r /tmp/requirements-lambda.txt boto3

COPY aws/handler.py ${LAMBDA_TASK_ROOT}/handler.py

CMD ["handler.lambda_handler"]
