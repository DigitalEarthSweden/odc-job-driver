 
  # -------------------------------------------------------------------------
  #  Control Panel Runtime Image
  # -------------------------------------------------------------------------
    FROM ubuntu:latest AS dapctrl
    
    # Install runtime dependencies with BuildKit cache optimization
    RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
        --mount=type=cache,target=/var/lib/apt,sharing=locked \
        apt-get update && apt-get upgrade -y && \
        apt-get install -y vim tree nano sudo tini wget &&\
        rm -rf /var/lib/apt/lists/*
    COPY bin/remap-user.sh  /usr/local/bin/remap-user.sh 

    # Remove this and sudo package when going for production
    RUN echo "ubuntu ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/ubuntu && \
    chmod 0440 /etc/sudoers.d/ubuntu

    # Switch to non-root user and install what we need for our pipelining and house-keeping
    USER ubuntu
    WORKDIR /home/ubuntu
    
    ENV PATH="/home/ubuntu/miniconda3/bin:${PATH}"
    
    ARG TARGETARCH
    RUN if [ "$TARGETARCH" = "arm64" ]; then \
            wget -O miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-aarch64.sh; \
        else \    
            wget -O miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh; \
        fi 
    
    RUN bash miniconda.sh -b && \
        rm -f miniconda.sh && \
        /home/ubuntu/miniconda3/bin/conda init && \
        . ~/.bashrc && \
        conda update conda -y && \
        conda install -y -c conda-forge mamba
    
    #Create the Conda environment
    COPY   environment.yml /home/ubuntu/environment.yml
    COPY --chown=1000:1000 .s3cfg /home/ubuntu/.s3cfg
    COPY . /app
    RUN conda env create -f /home/ubuntu/environment.yml && \
        conda clean -afy   

    # # Ensure conda environment is activated by default
    RUN echo "conda activate dap-lite" >> ~/.bashrc
   
    ENTRYPOINT ["/usr/local/bin/remap-user.sh"]
    # Note that we mount a lot of dirs during development so no need to 
    # copy that into the container (yet)

    ENV PATH="/app/bin:${PATH}"

    # Patch botocode for read only access, one of below file should work
    # Depeding on what base image is chosen. 
    USER root
    RUN find /usr/local/lib -path '*/botocore/handlers.py' -exec sed -i '/^VALID_BUCKET/c\VALID_BUCKET = re.compile(r"^[a-zA-Z0-9.\\-_:]{1,255}$")' {} \;
    #RUN find /virtualenv -path '*/botocore/handlers.py' -exec sed -i '/^VALID_BUCKET/c\VALID_BUCKET = re.compile(r"^[a-zA-Z0-9.\\-_:]{1,255}$")' {} \;
    
    USER ubuntu
    # Set working directory for runtime
    WORKDIR /app

    EXPOSE 8000

    # Run the FastAPI app with Uvicorn as the default command
    CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
    