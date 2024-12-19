# -------------------------------------------------------------------------
#  Control Panel Runtime Image
# -------------------------------------------------------------------------
    FROM ubuntu:latest AS dapctrl

    # Install runtime dependencies with BuildKit cache optimization
    RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
        --mount=type=cache,target=/var/lib/apt,sharing=locked \
        apt-get update && apt-get upgrade -y && \
        apt-get install -y vim tree nano sudo tini wget && \
        rm -rf /var/lib/apt/lists/*
    
    COPY bin/remap-user.sh /usr/local/bin/remap-user.sh
    
    # Add sudoers for non-root user (development only)
    RUN echo "ubuntu ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/ubuntu && \
        chmod 0440 /etc/sudoers.d/ubuntu
    
    # Switch to non-root user
    USER ubuntu
    WORKDIR /home/ubuntu
    
    # Install Miniconda based on the architecture
    ENV PATH="/home/ubuntu/miniconda3/bin:${PATH}"
    ARG TARGETARCH
    RUN if [ "$TARGETARCH" = "arm64" ]; then \
            wget -O miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-aarch64.sh; \
        else \
            wget -O miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh; \
        fi && \
        bash miniconda.sh -b && \
        rm -f miniconda.sh && \
        /home/ubuntu/miniconda3/bin/conda init && \
        conda update conda -y && \
        conda install -y -c conda-forge mamba && \
        rm -rf /home/ubuntu/miniconda3/pkgs/*
    
    # Create the Conda environment
    COPY environment.yml /home/ubuntu/environment.yml
    RUN conda env create -f /home/ubuntu/environment.yml && \
        conda clean -afy
    
    # Ensure Conda environment is activated by default
    RUN echo "conda activate dap-lite" >> ~/.bashrc
    
   
    USER ubuntu
    WORKDIR /app
    
    EXPOSE 8000
    
    # Run the FastAPI app using Conda
    CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
    