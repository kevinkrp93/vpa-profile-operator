FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api ./api
COPY controller ./controller
COPY cmd ./cmd
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/vpa-profile-operator ./cmd/operator

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/vpa-profile-operator /vpa-profile-operator
ENTRYPOINT ["/vpa-profile-operator"]
