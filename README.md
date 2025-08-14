### Annotation Service

Backend API.

_Supported OS:_ **Linux & Mac**

## Prerequisites

- Docker  
- Neo4j or Neo4j Aura account
- MORK server running
- MongoDB database  

**Follow these steps to run:**

1. **Clone the Repository**:

   ```sh
   git clone https://github.com/rejuve-bio/annotation-query-backend-general.git
   cd annotation-query-backend-general


2. **Set Up the Virtual Environment**:

   ```sh
   python -m venv venv
   source venv/bin/activate
   ```

3. **Install Dependencies**:

   ```sh
   pip install -r requirements.txt
   ```

4. **Configure Environment Variables**:
   Create a `.env` file in the root folder with the following content:

   ```plaintext
   NEO4J_URL=your_neo4j_url
   NEO4J_USERNAME=your_neo4j_user
   NEO4J_PASSWORD=your_neo4j_password
   MORK_URL=your_mork_url
   SCHEMA_DATA_VOLUME= # the file path where the output of the custom atomspace builder is stored
   MONGO_URI=your_mongodb_url
   LLM_MODEL=openai|gemini
   OPENAI_API_KEY=your_openai_api_key_if_applicable
   GEMINI_API_KEY=your_gemini_api_key_if_applicable
   ```

7. **Run the Application**:

   ```sh
   flask run
   ```

---

## API Documentation

* **Load Atomspace**

  Endpoint: `/annotation/load`
  Method: `POST`

  This endpoint loads the atomspace. The request body must include:

  | Field       | Description                                                        |
  | ----------- | ------------------------------------------------------------------ |
  | `folder_id` | The folder ID of the custom atomspace build                        |
  | `type`      | The type of atomspace; can be one of: `mork`, `metta`, or `cypher` |

  Example request body:

  ```json
  {
    "folder_id": "your_folder_id_here",
    "type": "mork"
  }
  ```

* **Query**

  Endpoint: `/query`
  Method: `POST`

  Use this endpoint to query the loaded atomspace.

---

# Running with Docker

**Build and Run Docker Container:**

```sh
docker build -t app .
docker run -p 5000:5000 app
```

This will expose the application on port 5000.

---

# Running with Docker Compose

Build and start services:

```bash
docker-compose up --build
```

Access the application through Caddy at:
`http://localhost:5000`

Stop services:

```bash
docker-compose down
```

---

# Running with `run.sh` script

Make sure these environment variables are set in your `.env` file:

```bash
APP_PORT=<the port on which the application will be exposed>
DOCKER_HUB_REPO=<Docker Hub repository in the format username/repository>
MONGODB_DOCKER_PORT=27017
CADDY_PORT=<the port on which Caddy will listen for incoming requests>
CADDY_PORT_FORWARD=<the internal port inside the Docker container where Caddy forwards requests>
```

## Script Usage

* **Run Containers**

  ```bash
  sudo ./run.sh run
  ```

* **Push Docker Images**

  ```bash
  sudo ./run.sh push
  ```

* **Clean Up**

  ```bash
  sudo ./run.sh clean
  ```

* **Stop Containers**

  ```bash
  sudo ./run.sh stop
  ```

* **Re-run Containers**

  ```bash
  sudo ./run.sh re-run
  ```

---
