#!/usr/bin/env python3
"""
PySpark Structured Streaming job for NLP processing of news content.
"""
import json
import logging
import os
from typing import List, Dict

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    ArrayType,
    MapType,
)

from config import settings

# Logging setup
logging.basicConfig(
    level=settings.app.log_level,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("NewsNLPProcessor")

# Import Hazm components for Persian NLP
try:
    import hazm

    # Initialize hazm components without download method
    normalizer = hazm.Normalizer()

    # Try to initialize POS tagger with default model path
    try:
        # Method 1: Try with default model (if installed via pip)
        tagger = hazm.POSTagger()
    except Exception as e:
        logger.warning(f"Could not load default POS tagger: {e}")
        try:
            # Method 2: Try with explicit model path if you have it
            model_path = os.path.expanduser("~/hazm_models/postagger.model")
            if os.path.exists(model_path):
                tagger = hazm.POSTagger(model=model_path)
            else:
                tagger = None
                logger.warning("POS tagger model not found, POS tagging will be disabled")
        except Exception as e2:
            logger.warning(f"Could not load POS tagger from path: {e2}")
            tagger = None

    # Initialize other components
    sent_tokenize = hazm.SentenceTokenizer()
    word_tokenize = hazm.WordTokenizer()

    # Try to initialize NER tagger
    try:
        # Method 1: Try with default model
        ner_tagger = hazm.NamedEntityRecognizer()
    except Exception as e:
        logger.warning(f"Could not load default NER tagger: {e}")
        try:
            # Method 2: Try with explicit model path if you have it
            ner_model_path = os.path.expanduser("~/hazm_models/ner.model")
            if os.path.exists(ner_model_path):
                ner_tagger = hazm.NamedEntityRecognizer(model=ner_model_path)
            else:
                ner_tagger = None
                logger.warning("NER model not found, NER will be disabled")
        except Exception as e2:
            logger.warning(f"Could not load NER tagger from path: {e2}")
            ner_tagger = None

except ImportError:
    logger.error(
        "Hazm library not found. Please install it with: pip install hazm"
    )
    normalizer = None
    tagger = None
    sent_tokenize = None
    word_tokenize = None
    ner_tagger = None


def get_spark_session(app_name: str) -> SparkSession:
    """
    Creates and returns a SparkSession.
    """
    logger.info(f"Creating SparkSession with app name '{app_name}'...")
    return (
        SparkSession.builder.appName(app_name)
        .master(settings.spark.master_url)
        .getOrCreate()
    )


def extract_entities(text: str) -> List[Dict[str, str]]:
    """
    Extracts named entities from Persian text using the hazm library.
    This function processes text and returns a list of dictionaries,
    each containing a found entity and its label.
    """
    if not normalizer or not word_tokenize:
        return []

    try:
        # Normalize the text to handle character variations and half-spaces
        normalized_text = normalizer.normalize(text)

        # Tokenize the text into words
        words = word_tokenize.tokenize(normalized_text)

        # If NER tagger is available, use it
        if ner_tagger:
            try:
                tagged_words = ner_tagger.tag(words)
                found_entities = []

                # Convert hazm's format to the desired output format
                for word, tag in tagged_words:
                    if tag and tag not in ["O"]:  # O typically means "Outside" (no entity)
                        found_entities.append({"text": word, "label": tag})

                return found_entities
            except Exception as e:
                logger.warning(f"NER tagging failed: {e}")

        # Fallback: Simple entity extraction using POS tags if available
        if tagger:
            try:
                pos_tagged = tagger.tag(words)
                found_entities = []

                for word, pos_tag in pos_tagged:
                    # Persian POS tags for potential entities
                    if pos_tag in ["N", "Ne"]:  # Noun, Proper noun
                        found_entities.append({"text": word, "label": "NOUN"})

                return found_entities
            except Exception as e:
                logger.warning(f"POS tagging failed: {e}")

        # Ultimate fallback: return empty list
        return []

    except Exception as e:
        logger.error(f"Entity extraction failed: {e}")
        return []


def extract_keywords(text: str) -> List[str]:
    """
    A simple keyword extraction function.
    In a real-world scenario, this would use a more advanced method (e.g., TF-IDF).
    """
    if not normalizer or not word_tokenize:
        # Fallback to simple split
        common_words = {"the", "a", "an", "is", "of", "in", "for", "and", "to", "را", "به", "از", "در", "که"}
        return [
            word.lower()
            for word in text.split()
            if len(word) > 3 and word.lower() not in common_words
        ]

    try:
        # Use hazm for better tokenization
        normalized_text = normalizer.normalize(text)
        words = word_tokenize.tokenize(normalized_text)

        # Persian and English stop words
        common_words = {
            "the", "a", "an", "is", "of", "in", "for", "and", "to",
            "را", "به", "از", "در", "که", "این", "آن", "با", "بر", "تا"
        }

        return [
            word.lower()
            for word in words
            if len(word) > 3 and word.lower() not in common_words
        ]
    except Exception as e:
        logger.warning(f"Keyword extraction failed, using fallback: {e}")
        common_words = {"the", "a", "an", "is", "of", "in", "for", "and", "to"}
        return [
            word.lower()
            for word in text.split()
            if len(word) > 3 and word.lower() not in common_words
        ]


def main():
    """
    Main function to run the Structured Streaming job.
    """
    spark = get_spark_session("NewsNLPProcessor")
    spark.sparkContext.setLogLevel(settings.app.log_level.upper())

    # Register UDFs
    ner_udf = udf(
        extract_entities, ArrayType(MapType(StringType(), StringType()))
    )
    keywords_udf = udf(extract_keywords, ArrayType(StringType()))

    # Define the schema of the incoming Kafka message
    schema = StructType(
        [
            StructField("url", StringType()),
            StructField("title", StringType()),
            StructField("summary", StringType()),
            StructField("published_at", TimestampType()),
            StructField("created_at", TimestampType()),
            StructField("content", StringType()),
            StructField("category", StringType()),
            StructField("source", StringType()),
        ]
    )

    logger.info(
        "Reading stream from Redpanda topic '%s'...",
        settings.redpanda.news_content_topic,
    )

    # Read the streaming data from Kafka (Redpanda)
    kafka_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", f"redpanda:{settings.redpanda.internal_port}")
        .option("subscribe", settings.redpanda.news_content_topic)
        .load()
    )

    # Parse the JSON from the Kafka message and apply NLP transformations
    parsed_df = kafka_stream_df.selectExpr("CAST(value AS STRING)")
    news_df = (
        parsed_df.select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
        .filter(col("content").isNotNull())
    )

    # Apply NLP UDFs
    transformed_df = news_df.withColumn(
        "ner_entities", ner_udf(col("content"))
    ).withColumn("keywords", keywords_udf(col("content")))

    # Prepare data for output
    output_df = transformed_df.select(
        "url",
        "title",
        "summary",
        "ner_entities",
        "keywords",
    )

    # Write the enriched data back to the console for demonstration
    query = (
        output_df.writeStream.outputMode("append")
        .format("console")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()