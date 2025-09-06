# FROM rust:latest
FROM public.ecr.aws/b8h3z2a1/sravz/backend-rust:v29

# # Enable for complete install from base
# # Install lldb
# RUN apt-get update && \
#     apt-get install -y lldb python3-pip && \
#     rm -rf /var/lib/apt/lists/*

# # RUN python3 -m venv myenv && \
# #     source env/bin/activate && \
# RUN mv /usr/lib/python3.11/EXTERNALLY-MANAGED /usr/lib/python3.11/EXTERNALLY-MANAGED.old

# # Install lldb
# RUN apt-get update && \
#     apt-get install -y lldb python3-pip && \
#     rm -rf /var/lib/apt/lists/*

# RUN useradd -m -r -s /bin/bash appuser
# COPY requirements.txt  /tmp/
# # Enable if pip packages to be installed
# RUN pip install --no-input -r /tmp/requirements.txt 

USER appuser
WORKDIR /app
COPY . .
# RUN chown -R appuser:appuser /app

# Build the release version of the application
RUN cargo build --release

# Move the file outside target. Target directory will be deleted
RUN cp /app/target/release/sravz sravz

# Clean up
RUN find ./ -type f -name "*.rs" -exec rm -f {} \;
RUN rm Cargo.* Dockerfile env Makefile *.md *.txt
RUN rm -rf tests target

HEALTHCHECK  --interval=10m --timeout=30s CMD bash healthcheck.sh > /dev/null || exit 1

# Run sravz on start up
CMD ["./sravz"]
