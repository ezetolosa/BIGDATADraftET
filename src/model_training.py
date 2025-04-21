# src/model_training.py
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import os
import logging
from datetime import datetime

class ModelTrainer:
    def __init__(self, spark):
        self.spark = spark

    def _assemble_features(self, df, label_col):
        features = [
            "home_team_goal_rolling_avg",
            "away_team_goal_rolling_avg",
            "home_team_conceded_avg",
            "away_team_conceded_avg",
            "home_team_form",
            "away_team_form"
        ]
        df = df.dropna(subset=features + [label_col])
        assembler = VectorAssembler(inputCols=features, outputCol="features")
        return assembler.transform(df), features

    def train_result_classifier(self, df):
        df = df.withColumn("label",
            when(col("home_team_goal") > col("away_team_goal"), 2)
            .when(col("home_team_goal") < col("away_team_goal"), 0)
            .otherwise(1))

        assembled_df, _ = self._assemble_features(df, "label")
        train, test = assembled_df.randomSplit([0.8, 0.2], seed=42)
        rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=100)
        model = rf.fit(train)
        return model, test

    def evaluate_classifier(self, model, test_df):
        evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
        return evaluator.evaluate(model.transform(test_df))

    def train_goal_regressor(self, df):
        df = df.withColumnRenamed("home_team_goal", "label")
        assembled_df, _ = self._assemble_features(df, "label")
        rf = RandomForestRegressor(labelCol="label", featuresCol="features", numTrees=100)
        return rf.fit(assembled_df)

    def save_model(self, model, path):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        full_path = os.path.join(path + f"_{timestamp}")
        model.write().overwrite().save(full_path)

    def load_model(self, path):
        return PipelineModel.load(path)
