# Shell Scripting Utilities

```ascii
██████╗  █████╗ ███████╗██╗  ██╗
██╔══██╗██╔══██╗██╔════╝██║  ██║
██████╔╝███████║███████╗███████║
██╔══██╗██╔══██║╚════██║██╔══██║
██████╔╝██║  ██║███████║██║  ██║
╚═════╝ ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝
```

A collection of Bash utility scripts for automated environment setup on Debian/Ubuntu systems.

---

## Prerequisites

| Requirement | Version | Scripts |
|---|---|---|
| Bash | 4.0+ | all |
| `curl` / `wget` | any | `install_spark.sh` |
| Java (JDK/JRE) | 8+ | `install_spark.sh` |
| `apt-get` | any | `install_pkgs.sh` |
| Root / sudo | — | all |

---

## Scripts

### `install_pkgs.sh` — Batch Package Installer

Installs multiple `apt` packages in a single command. Skips packages that are already installed and prints a summary at the end.

**Usage**

```bash
sudo ./install_pkgs.sh pkg1 pkg2 pkg3 ...
```

**Examples**

```bash
# Install common dev tools
sudo ./install_pkgs.sh curl wget git vim build-essential

# Install Python and Java
sudo ./install_pkgs.sh python3 python3-pip default-jdk
```

**Output format**

```
[SKIP] curl is already installed
[INFO] Installing wget ...
[OK]   wget installed successfully
[FAIL] nonexistent-pkg failed to install

=== Summary: 1 installed, 1 skipped, 1 failed ===
```

**Exit codes**

| Code | Meaning |
|---|---|
| 0 | All operations completed |
| 1 | No arguments provided |
| 2 | Not running as root |

---

### `install_spark.sh` — Apache Spark Installer

```ascii
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\
      /_/
```

Automates Apache Spark installation: scrapes available versions from Apache mirrors, downloads the selected tarball, verifies the SHA-512 checksum, extracts to `/usr/local/spark`, and configures environment variables in `~/.profile`.

**What it does**

1. Validates root access and Java installation
2. Fetches available Spark versions from `downloads.apache.org`
3. Prompts for version selection (or accepts one as an argument)
4. Downloads the `.tgz` and `.sha512` from Apache mirrors
5. Verifies integrity with `sha512sum`
6. Extracts to `/usr/local/spark` (skips if already exists)
7. Appends `SPARK_HOME`, `PATH`, and `PYSPARK_PYTHON` to `~/.profile` (idempotent)
8. Applies PySpark or SparkR-specific config based on the selected package

**Usage — interactive**

```bash
sudo ./install_spark.sh
# Presents a numbered menu of available versions
```

**Usage — non-interactive (e.g. Dockerfile)**

```bash
sudo ./install_spark.sh spark-3.5.1-bin-hadoop3.tgz
```

**Docker example**

```dockerfile
FROM ubuntu:latest
RUN apt-get update && apt-get install -y curl wget default-jdk
COPY install_spark.sh /spark/
RUN /spark/install_spark.sh "spark-3.5.1-bin-hadoop3.tgz"
```

**Environment variables configured**

| Variable | Value |
|---|---|
| `SPARK_HOME` | `/usr/local/spark` |
| `PATH` | `$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin` |
| `PYSPARK_PYTHON` | `/usr/bin/python3` |
| `PYTHONPATH` | `$SPARK_HOME/python/lib/*.zip` (PySpark only) |

After installation, reload your shell:

```bash
source ~/.profile
```

---

## Additional Configuration

### Start a standalone master server

```bash
sudo $SPARK_HOME/sbin/start-master.sh
# Browse to http://localhost:8080 for the status UI
```

### Start a worker process

```bash
sudo $SPARK_HOME/sbin/start-worker.sh spark://localhost:7077
```

### Spark Shell (Scala)

```bash
$SPARK_HOME/bin/spark-shell
```

### PySpark

```bash
$SPARK_HOME/bin/pyspark
```

### SparkR

```bash
$SPARK_HOME/bin/sparkR
```

### Jupyter integration (spylon-kernel)

```bash
pip install spylon-kernel
python -m spylon_kernel install --user
# Then launch Jupyter and select the spylon-kernel (Scala + Spark)
```

---

## Linting

Both scripts are checked with [ShellCheck](https://www.shellcheck.net/). A `.shellcheckrc` is included at the project root:

```bash
# Install shellcheck
apt-get install -y shellcheck   # Debian/Ubuntu
brew install shellcheck          # macOS

# Run
shellcheck install_pkgs.sh install_spark.sh
```

---

## Links

- [Apache Spark Downloads](https://spark.apache.org/downloads.html)
- [Installing Spark on Ubuntu](https://datawookie.netlify.app/blog/2017/07/installing-spark-on-ubuntu/)
- [Spark Cluster Setup](https://www.tutorialkart.com/apache-spark/how-to-setup-an-apache-spark-cluster/)
- [Spark + Jupyter Notebook](https://medium.com/@am.benatmane/setting-up-a-spark-environment-with-jupyter-notebook-and-apache-zeppelin-on-ubuntu-e12116d6539e)
- [Scala Spark Shell Commands](https://data-flair.training/blogs/scala-spark-shell-commands/)
