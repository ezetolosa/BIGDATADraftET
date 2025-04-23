# src/model_training.py
from pyspark.sql.functions import when, col
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import os
import logging
from datetime import datetime

class ModelTrainer:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.logger = logging.getLogger(__name__)
        
        # Reduced feature set for faster processing
        self.feature_columns = [
            'home_team_goal_rolling_avg',
            'away_team_goal_rolling_avg',
            'home_team_form',
            'away_team_form'
        ]

    def _assemble_features(self, df, label_col):
        """Assemble features into a vector for ML training"""
        try:
            # Create feature vector
            assembler = VectorAssembler(
                inputCols=self.feature_columns,
                outputCol="features"
            )
            
            # Drop any rows with null values
            df = df.dropna(subset=self.feature_columns + [label_col])
            
            # Assemble features
            assembled_df = assembler.transform(df)
            
            return assembled_df, self.feature_columns
            
        except Exception as e:
            self.logger.error(f"Feature assembly failed: {e}")
            raise

    def train_result_classifier(self, df):
        try:
            # Cache with memory and disk
            df.persist()
            
            # Prepare features
            assembler = VectorAssembler(
                inputCols=self.feature_columns,
                outputCol="features"
            )
            
            # Drop nulls and assemble features
            df = df.dropna(subset=self.feature_columns + ["match_outcome"])
            assembled_df = assembler.transform(df)
            
            # Use smaller train/test split ratio
            train_data, test_data = assembled_df.randomSplit([0.7, 0.3], seed=42)
            
            # Create model with memory-optimized parameters
            rf = RandomForestClassifier(
                labelCol="match_outcome",
                featuresCol="features",
                numTrees=50,
                maxDepth=10,
                maxBins=32,
                minInstancesPerNode=2,
                seed=42
            )
            
            self.logger.info("Training model with optimized parameters...")
            model = rf.fit(train_data)
            
            # Generate predictions in batches
            predictions = model.transform(test_data)
            
            # Calculate metrics
            evaluator = MulticlassClassificationEvaluator(
                labelCol="match_outcome",
                predictionCol="prediction"
            )
            
            metrics = ['accuracy', 'f1']
            for metric in metrics:
                evaluator.setMetricName(metric)
                score = evaluator.evaluate(predictions)
                self.logger.info(f"Model {metric}: {score:.4f}")
            
            # Unpersist cached data
            df.unpersist()
            
            return model, predictions
            
        except Exception as e:
            self.logger.error(f"Model training failed: {e}")
            raise

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
