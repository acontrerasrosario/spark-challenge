import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, length, to_json, struct, lit


def _get_error_identifier(_field):
    return {
        "notEmpty": {"code": "1", "message": f"[{_field}] -- [IsEmpty]"},
        "notNull": {"code": "2", "message": f"[{_field}] -- [IsNull]"}
    }


def _get_sources(_metadata):
    _data_frames = {}
    for source in _metadata:
        source_name = source.get("name")
        source_path = source.get("path")
        source_format = source.get("format")
        _data_frames[source_name] = spark.read.format(source_format).load(source_path)
    return _data_frames


def _process_transformation(_input_dataframes, _output_dataframes, _metadata):
    _type = _metadata.get("type")
    if _type == "validate_fields":
        params = transformation.get("params", {})
        _df_ok = _input_dataframes[params.get("input")]
        _df_ko = _input_dataframes[params.get("input")]
        # Perform validations here
        fields = params.get("validations", [])
        for field in fields:
            field_name = field.get("field")
            validations = field.get("validations", [])
            for validation in validations:
                if validation == "notEmpty":
                    _df_ok = _df_ok.filter(length(_df_ok[field_name]) > 0)
                    _df_ko1 = (_df_ko
                               .filter(length(_df_ok[field_name]) == 0)
                               .withColumn("arraycoderrorbyfield",
                                           lit(json.dumps(_get_error_identifier(field_name).get(validation)))))
                elif validation == "notNull":
                    _df_ok = _df_ok.filter(_df_ok[field_name].isNotNull())
                    _df_ko2 = (_df_ko
                               .filter(_df_ko[field_name].isNull())
                               .withColumn("arraycoderrorbyfield",
                                           lit(json.dumps(_get_error_identifier(field_name).get(validation)))))
        _output_dataframes[f"{params.get('input')}-{params.get('output_success')}"] = _df_ok
        _output_dataframes[f"{params.get('input')}-{params.get('output_error')}"] = _df_ko1.union(_df_ko2)

    elif _type == "add_fields":
        params = transformation.get("params", {})
        _df = output_dfs[params.get("input")]
        addFields = params.get("addFields", [])
        for field in addFields:
            field_name = field.get("name")
            field_function = field.get("function")
            _df = _df.withColumn(field_name, current_timestamp())
        output_dfs[params.get('output')] = _df


def _process_sink(_output_dataframes, _metadata):
    sink_input = _metadata.get("input")
    sink_name = _metadata.get("name")
    sink_format = _metadata.get("format")

    # Write data to sink
    if sink_format == "KAFKA":
        # Write to Kafka topic
        boostrap_servers = _metadata.get("boostrap-servers")
        _output_df = output_dfs[sink_input].select(to_json(struct("age", "name", "office", "dt")).alias("value"))
        _output_df = _output_df.selectExpr("*", f"'{sink_name}' as key")
        print(_output_df.show())
        _output_df.printSchema()
        _output_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", boostrap_servers) \
            .option("topic", sink_name) \
            .save()
    elif sink_format == "JSON":
        # Write to JSON file
        sink_output_paths = _metadata.get("paths", [])
        sink_saveMode = _metadata.get("saveMode")
        for output_path in sink_output_paths:
            output_dfs[sink_input].write.mode(sink_saveMode).json(output_path)


metadata_file = "./metadata.json"
spark = (SparkSession
         .builder
         .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2')
         .appName("sdg-test")
         .getOrCreate())

metadata = json.loads(open(metadata_file, 'r').read())

dataflows = metadata.get("dataflows", [])
for dataflow in dataflows:
    sources = dataflow.get("sources", [])
    sources_dfs = _get_sources(sources)
    output_dfs = {}

    transformations = dataflow.get("transformations", [])
    for transformation in transformations:
        _process_transformation(sources_dfs, output_dfs, transformation)

    # Write to sinks
    sinks = dataflow.get("sinks", [])
    for sink in sinks:
        _process_sink(output_dfs, sink)

# Stop the SparkSession
spark.stop()
