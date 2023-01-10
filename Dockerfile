FROM python:3.11.1-alpine
WORKDIR /project
COPY *.py ./
EXPOSE 8080 5000/udp
ENTRYPOINT [ "python3", "/project/current.py" ]
