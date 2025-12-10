import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, lower, collect_set, size, first, countDistinct, count
import pandas as pd
from pyspark.sql import Window



# --- File Configuration 
RAW_INPUT_FILE = "raw_annotations.csv"
CLEAN_OUTPUT_FILE = "clean_training_dataset.jsonl"
DISAGREEMENT_LOG_FILE = "disagreements.log"
MIN_CONFIDENCE_SCORE = 0.8


# --- Utility Functions ---

def create_spark_session():

    """Initializes and returns a PySpark SparkSession."""

    return SparkSession.builder \
        .appName("Text_Annoation_pipeline") \
        .master("local[*]") \
        .getOrCreate()


def read_data_to_dataframe(spark: SparkSession, file_path: str) -> DataFrame:

    """Reads the CSV file into a Spark DataFrame"""

    print(f"Reading data from: {file_path}")
    try:
        # Use header=True and inferSchema=True for robust CSV reading
        df = spark.read.csv(file_path, header=True, inferSchema=True)

        df.show()
        print(f"Successfully loaded {df.count()} rows.")
        return df
    except Exception as e:
        print(f"FATAL ERROR: Could not read CSV file at '{file_path}'. Please check the path and file existence.")
        raise e


# --- Quality Validator and Output Generator ---
'''
def quality_validator(raw_df: DataFrame):
    """
    Applies confidence filtering and checks inter-annotator agreement.
    Returns: agreed_df, disagreed_df
    """
    print("\n--- Running Quality Validator ---")

    # 1. Quality Check 1: Confidence Filtering
    high_confidence_df = raw_df.filter(col("confidence_score") >= MIN_CONFIDENCE_SCORE)

    high_confidence_df.show(False)
    text_label_counts = high_confidence_df.groupby('text')['label'].nunique()

    text_label_counts.show(False)
    disagreed_texts = text_label_counts[text_label_counts > 1].index.tolist()

    # Separate into agreed and disagreed dataframes
    disagreed_df = high_confidence_df[high_confidence_df['text'].isin(disagreed_texts)].copy()
    agreed_df = high_confidence_df[~high_confidence_df['text'].isin(disagreed_texts)].copy()

    agreed_df.show(5,False)
    disagreed_df.show(5,False)

    return agreed_df, disagreed_df

'''

def quality_validator(raw_df: DataFrame):
    """
    Applies confidence filtering and checks inter-annotator agreement.

    Returns:
        tuple: (agreed_df, disagreed_df)
            - agreed_df: Unique Texts where all annotators agree on label
            - disagreed_df: Texts where annotators disagree on label
    """
    print("\n--- Running Quality Validator ---")

    # Quality Check 1: Confidence Filtering
    high_confidence_df = raw_df.filter(col("confidence_score") >= MIN_CONFIDENCE_SCORE)

    filtered_count = raw_df.count() - high_confidence_df.count()

    
    print(f"Filtered {filtered_count} low-confidence annotations")

    window_spec = Window.partitionBy("text")

    df_with_label_count = high_confidence_df.withColumn(
            "unique_labels",
            collect_set("label").over(window_spec)
        ).withColumn(
            "distinct_labels",
            size("unique_labels")
        ).drop("unique_labels")
        
    df_with_label_count.show(10, False)
    

    df_with_label_count.show(10,False)

    # Agreed: Only one unique label per text
    agreed_df = df_with_label_count.filter(col("distinct_labels") == 1) \
                                   .drop("distinct_labels") \
                                   .dropDuplicates(["text", "label"])

    # Disagreed: Multiple labels for same text
    disagreed_df = df_with_label_count.filter(col("distinct_labels") > 1) \
                                      .drop("distinct_labels")

    print(f"Agreement Check complete:")
    print(f"  - Agreed samples: {agreed_df.count()}")
    print(f"  - Disagreed samples: {disagreed_df.count()}")

    return agreed_df, disagreed_df


def output_generator(agreed_df: DataFrame, disagreed_df: DataFrame, log_file: str, output_file: str):

    """Logs disagreement failures and writes the final clean training data."""

    print("\n--- Running Output Generator ---")

    try:
        agreed_pandas_df = agreed_df.select(col("text"), col("label")).toPandas()
        disagreed_pandas_df = disagreed_df.select(col("text"),col("annotator_id"), col("label"),col("confidence_score")).toPandas()
    except Exception as e:
        print(f"ERROR: Failed to convert to Pandas. Data may be too large for driver memory. Error: {e}")
        return

    if len(disagreed_pandas_df) > 0:
        with open(DISAGREEMENT_LOG_FILE, 'w') as f:
            for index, row in disagreed_pandas_df.iterrows():
                json_record = json.dumps(row.to_dict())
                f.write(json_record + '\n')

    if len(agreed_pandas_df) > 0:
        with open(CLEAN_OUTPUT_FILE, 'w') as f:
                for index, row in agreed_pandas_df.iterrows():
                    json_record = json.dumps(row.to_dict())
                    f.write(json_record + '\n')
    
    print(f"Clean dataset saved to: {output_file}")





# --- Main Execution Logic ---

if __name__ == "__main__":
    
    # 1. Setup Spark
    spark = create_spark_session()
    
    # 2. Read the raw annotations (Directly loads the CSV)
    raw_df = read_data_to_dataframe(spark, RAW_INPUT_FILE)
    
    # 3. Validation Pipeline
    agreed_samples, disagreed_samples = quality_validator(raw_df)

    # 4. Output Generation
    output_generator(agreed_samples, disagreed_samples, DISAGREEMENT_LOG_FILE, CLEAN_OUTPUT_FILE)
    
    # 5. Cleanup
    spark.stop()
    
    print("\n--- Processing Complete ---")