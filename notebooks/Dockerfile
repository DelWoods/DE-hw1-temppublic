FROM python:3.11-slim

WORKDIR /app

# only notebook deps, the repo itself is mounted at runtime:
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app/notebooks

# run the notebook top to bottom, outputs are saved back into the mounted file:
CMD ["jupyter", "nbconvert", "--execute", "--to", "notebook", "--inplace", "etl.ipynb"]
