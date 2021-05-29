FROM tiangolo/uwsgi-nginx-flask:python3.8-alpine
# FROM python:3.6-alpine
WORKDIR /app
COPY requirements.txt .
# RUN apk add --no-cache gcc make libc-dev linux-headers pcre-dev
# RUN pip install -r requirements.txt -i https://mirrors.bfsu.edu.cn/pypi/web/simple && pip install uwsgi -i https://mirrors.bfsu.edu.cn/pypi/web/simple
RUN pip install -r requirements.txt -i https://mirrors.bfsu.edu.cn/pypi/web/simple
COPY . .
# CMD ["uwsgi", "--ini", "uwsgi.ini"]
ENV LISTEN_PORT 5000
EXPOSE 5000

