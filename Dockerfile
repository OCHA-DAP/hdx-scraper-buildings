FROM public.ecr.aws/unocha/python:3.13-stable

WORKDIR /srv

ENV LLVM_CONFIG=/usr/bin/llvm-config-15

RUN --mount=type=bind,source=requirements.txt,target=requirements.txt \
    apk add --no-cache \
    gdal-driver-parquet \
    gdal-tools && \
    apk add --no-cache --virtual .build-deps \
    apache-arrow-dev \
    build-base \
    cmake \
    gdal-dev \
    geos-dev \
    llvm15-dev && \
    pip install --no-cache-dir -r requirements.txt && \
    apk del .build-deps && \
    rm -rf /var/lib/apk/*

COPY src ./

CMD ["python", "-m", "hdx.scraper.buildings"]
