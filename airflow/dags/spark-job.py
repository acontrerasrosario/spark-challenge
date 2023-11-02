import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, length

metadata_file = "./metadata.json"
spark = (SparkSession
         .builder
         .appName("sdg-test")
         .getOrCreate())

metadata = json.loads(metadata_file)

dataflows = metadata.get("dataflows", [])
for dataflow in dataflows:
    sources = dataflow.get("sources", [])
    sources_dfs = {}
    output_dfs = {}
    for source in sources:
        source_name = source.get("name")
        source_path = source.get("path")
        source_format = source.get("format")
        sources_dfs[source_name] = spark.read.format(source_format).load(source_path)
    
    transformations = dataflow.get("transformations", [])
    for transformation in transformations:
        transformation_type = transformation.get("type")
        if transformation_type == "validate_fields":
            params = transformation.get("params", {})
            _df = sources_dfs[params.get("input")]
            # Perform validations here
            fields = params.get("validations", [])
            for field in fields:
                field_name = field.get("field")
                validations = field.get("validations", [])
                for validation in validations:
                    if validation == "notEmpty":
                        _df = _df.filter(length(_df[field_name]) > 0)
                    elif validation == "notNull":
                        _df = _df.filter(_df[field_name].isNotNull())
            output_dfs[f"{params.get('input')}-ok_with_date"] = _df

        elif transformation_type == "add_fields":
            params = transformation.get("params", {})
            _df = sources_dfs[params.get("input")]
            addFields = params.get("addFields", [])
            for field in addFields:
                field_name = field.get("name")
                field_function = field.get("function")
                _df = _df.withColumn(field_name, current_timestamp())
            output_dfs[f"{params.get('input')}-ok_with_date"] = _df

    # Write to sinks
    sinks = dataflow.get("sinks", [])
    for sink in sinks:
        sink_input = sink.get("input")
        sink_name = sink.get("name")
        sink_format = sink.get("format")

        # Write data to sink
        output_path = "/data/output/" + sink_name
        if sink_format == "KAFKA":
            # Write to Kafka topic
            output_dfs[sink_input].selectExpr("*", f"'{sink_name}' as key").write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "your_kafka_brokers") \
                .option("topic", sink_name) \
                .save()
        elif sink_format == "JSON":
            # Write to JSON file
            output_dfs[sink_input].write.mode("overwrite").json(output_path)

# Stop the SparkSession
spark.stop()
