FROM golang:1.19-alpine AS builder

# Install git for go modules
RUN apk add --no-cache git

# Set Go proxy to direct to avoid proxy issues
ENV GOPROXY=direct
ENV GOSUMDB=off

WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/

COPY --from=builder /app/main .

EXPOSE 8082

CMD ["./main"]