FROM golang:1.22

WORKDIR /app

COPY . .

RUN go build -o dbms .

EXPOSE 8080

CMD ["./dbms"]