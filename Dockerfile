# FROM tiangolo/uwsgi-nginx:python3.6-alpine3.8
FROM python:3.6-alpine
WORKDIR /app
COPY requirements.txt .
RUN apk add --no-cache gcc make libc-dev linux-headers pcre-dev
RUN pip install -r requirements.txt -i https://mirrors.bfsu.edu.cn/pypi/web/simple
COPY . .
CMD ["uwsgi", "--ini", "uwsgi.ini"]
# EXPOSE 80

