FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /desk
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && useradd --uid 10001 --create-home desk
COPY --chown=desk:desk server.py engine.py feeds.py papercheck.py store.py backup.py ./
COPY --chown=desk:desk public ./public
RUN mkdir -p runtime public/assets/tokens && chown -R desk:desk runtime public/assets/tokens
USER desk
EXPOSE 8080
CMD ["python", "server.py"]
