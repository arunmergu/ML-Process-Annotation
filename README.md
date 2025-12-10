Prerequisites:
Python 11
Spark 4.0
Java 17

step1: Clone the git project using below command 
git clone https://github.com/arunmergu/ML-Process-Annotation.git


step2: Install requirements.txt file. 
pip3 install requirements.txt

Step3: Run the process_annotations.py script to generate clean_training_data.jsonl and disagreements.log

python3 process_annotations.py



How PoC fits into larger system?

system design represents the specialized Feature Pipeline, serving as the critical bridge that connects the messy world of raw data and human annotations to the consumer layer of ML models. It is architecturally engineered for scalability and reliability, relying on distributed processing via Apache Spark (Dataproc) to handle high-volume data transformation. The Airflow/Composer orchestrator automates the entire flow, managing data ingestion and crucial backfill operations for historical datasets. The system’s core value lies in the Offline Feature Store (BigQuery), which guarantees point-in-time correctness by storing time-versioned feature data, preventing data leakage during model training. These validated features are then synced to the Online Feature Store (Memorystore) for ultra-low-latency serving to real-time models. Furthermore, robust governance is built into the pipeline, utilizing metadata to track data lineage and feature code versions, ensuring auditability and compliance throughout the data lifecycle. Although implemented primarily with GCP cloud services for its managed benefits, the underlying open-source technologies (Spark, Airflow) ensure the same robust design can be translated to other cloud environments or custom infrastructure based on trade-off analysis.





If java is not installed then below are the steps to install (MAC OS)
------------------------------------------------------------
Step 1: Install Compatible Java (Java 17)

You need to install the Java version that Spark 4.0.0 requires.

brew install openjdk@17

Step 2: Set JAVA_HOME

Since you installed Java 17 via Homebrew, you need to link it and set your environment variables to ensure PySpark uses the correct path.

sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk

Open your shell configuration file (~/.zshrc or ~/.bash_profile):

nano ~/.zshrc

Add/Update the JAVA_HOME export:

# Setting JAVA_HOME for Java 17 installed via Homebrew
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
export PATH="$JAVA_HOME/bin:$PATH"

Save, close, and apply changes:

source ~/.zshrc

Step 3: Install Apache Spark

brew install apache-spark

Step 4: Set SPARK_HOME

After installation, Homebrew places the actual executable files in a specific directory. You must set SPARK_HOME to point to this location.

SPARK_PATH=$(brew --prefix apache-spark)/libexec
echo $SPARK_PATH

Add/Update SPARK_HOME in your shell configuration file (~/.zshrc):

nano ~/.zshrc

export SPARK_HOME="$SPARK_PATH"
export PATH="$SPARK_HOME/bin:$PATH"

Apply changes one final time:

source ~/.zshrc


