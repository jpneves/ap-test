# Dagster + dbt Project - Alternative Payments test

This repository contains a [Dagster](https://dagster.io/) data pipeline integrated with [dbt](https://www.getdbt.com/) for local development and testing using Docker.

## 🐳 Requirements

* [Docker](https://www.docker.com/)
* [Docker Compose](https://docs.docker.com/compose/install/) (usually comes with Docker Desktop)

---

## ✨ Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/jpneves/ap-test.git
cd ap-test
```

---

### 2. Project Structure

```
.
├── dbt_analytics/
│   └── ... (dbt models)
├── docker-compose.yml
│   Dockerfile-code
│   Dockerfile-dagster
│   pipeline.py
│   requirements.txt
│   workspace.yaml
└── README.md
```

### 3. Step: Update Absolute Path in dagster.yaml

`dagster.yaml` is interpreted inside the container or Dagster process, so any path must be valid in that environment. 
You must use absolute container paths. 

In the `dagster.yaml` file, locate the following line:

```yaml
    - ABSOLUTE_PATH:/opt/dagster/app
```
Replace ABSOLUTE_PATH with the absolute path to your local project directory. For example:

```yaml
    - /home/your-username/projects/my-dagster-repo:/opt/dagster/app
```

📌 This is necessary to mount your local code inside the container so Dagster can access your project files.

---

### 4. Build and Start the Containers

```bash
docker-compose up --build
```

This will:

* Spin up Dagster UI at [http://localhost:3000](http://localhost:3000)
* Mount your local files for live development

---

### 5. Activate the Job in Dagster UI

Once the Dagster UI is running at http://localhost:3000, follow these steps to activate the scheduled job:

1. Navigate to the Automation section.
2. Locate the schedule named daily_pipeline_job_schedule.
3. Click the toggle to activate the schedule.
4. You can monitor upcoming runs or trigger a run manually from the Runs tab or directly from the schedule page.

---

## 🧪 Troubleshooting

* **Port already in use?**
  Make sure nothing else is running on ports `3000` (Dagster UI)  and `5432`(Postgres).

* **dbt errors?**
  Ensure your `profiles.yml` is correctly set up and matches the container's file paths.
