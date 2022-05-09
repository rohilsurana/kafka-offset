FROM alpine:3.15

COPY kafka-offset /usr/bin/kafka-offset

RUN apk --no-cache add ca-certificates bash

CMD ["kafka-offset"]
