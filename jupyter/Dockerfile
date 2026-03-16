FROM jupyter/base-notebook:python-3.11

# ===== Switch to root to install packages =====
USER root

# (optional) system deps
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# ===== Python dependencies =====
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ===== Back to jovyan user =====
USER jovyan

# ===== Working directory =====
WORKDIR /home/jovyan/work
