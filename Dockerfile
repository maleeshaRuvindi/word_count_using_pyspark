FROM debian:bullseye

# Install system dependencies
# Install necessary dependencies
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    bzip2 \
    ca-certificates \
    git \
    openjdk-11-jdk && \
    procps && \  
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
ENV PATH="${JAVA_HOME}/bin:${PATH}"


# Install Miniconda
RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /miniconda.sh && \
    chmod +x /miniconda.sh && \
    /miniconda.sh -b -p /opt/miniconda && \
    rm /miniconda.sh

# Set Conda path
ENV PATH="/opt/miniconda/bin:$PATH"

# Initialize Conda
RUN /opt/miniconda/bin/conda init bash

# Create Conda environment
RUN conda create --name pyspark_env python=3.11 -y

# Set working directory
WORKDIR /app

# Copy project files
COPY . /app/

# Install Python dependencies inside Conda environment
RUN conda run -n pyspark_env pip install \
    pyspark \
    # torch \
    # numpy \
    pandas \
    # scipy \
    # scikit-learn \
    # polars \
    # orjson \
    pyarrow \
    # awswrangler \
    # transformers \
    # accelerate \
    # duckdb \
    # neo4j \
    # s3fs \
    # umap-learn \
    # smart-open \
    # onnxruntime \
    # spacy \
    # seqeval \
    # gensim \
    # numba \
    # sqlalchemy \
    # pytest \
    datasets\
    pyyaml

# Generate a build.txt file with timestamp and details
# RUN echo "Build Timestamp: $(date -u)" > /app/build.txt && \
#     echo "Docker Image: pyspark-container" >> /app/build.txt && \
#     echo "Python Version: $(python --version)" >> /app/build.txt && \
#     echo "Installed Packages:" >> /app/build.txt && \
#     conda run -n pyspark_env pip freeze >> /app/build.txt

# Give execute permissions to the script
# RUN chmod +x /app/script/run.sh

# Ensure Conda environment is correctly sourced before running
# ENTRYPOINT ["/bin/bash", "-c", "source activate pyspark_env && /app/script/run.sh"]
CMD ["/bin/bash"]
