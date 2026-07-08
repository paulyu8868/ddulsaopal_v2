# Lambda 컨테이너 이미지 — 의존성 + 래퍼만 포함 (매매 코드는 런타임에 main을 clone)
# 빌드 컨텍스트는 레포 루트: docker build -f aws/Dockerfile .
FROM public.ecr.aws/lambda/python:3.10

RUN yum install -y git && yum clean all

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt boto3

COPY aws/handler.py ${LAMBDA_TASK_ROOT}/handler.py

CMD ["handler.lambda_handler"]
