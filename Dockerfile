# FROM rust:latest
FROM public.ecr.aws/b8h3z2a1/sravz/backend-rust:v29

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
RUN rm -f Cargo.* Dockerfile env Makefile *.md *.txt
RUN rm -rf tests target

HEALTHCHECK  --interval=10m --timeout=30s CMD bash healthcheck.sh > /dev/null || exit 1

# Run sravz on start up
CMD ["./sravz"]
