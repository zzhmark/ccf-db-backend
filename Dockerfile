FROM tiangolo/uwsgi-nginx:python3.6-alpine3.8
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt -i https://mirrors.bfsu.edu.cn/pypi/web/simple
COPY . .
EXPOSE 80

