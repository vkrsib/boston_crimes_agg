# boston_crimes_agg

Create aggregates of Boston crimes (https://www.kaggle.com/AnalyzeBoston/crimes-in-boston) via Spark.

## Usage

1. Build the fat JAR
   ```
   sbt assembly
   ```

2. Submit the application to Spark
   ```
   spark-submit --master "local[*]" \
   --class com.example.BostonCrimesMap \
   ./target/scala-2.12/boston_crimes_agg-assembly-0.1.jar \
   ./data/crime.csv \
   ./data/offense_codes.csv \
   ./data
   ```
   