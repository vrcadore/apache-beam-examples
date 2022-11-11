#!make
-include .env
export

help: ## Display this help screen.
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

pre-commit: ## Runs the pre-commit over entire repo.
	@pdm run pre-commit run --all-files

unit-tests: ## Runs unit tests for pipelines.
	@pdm run python -m pytest

setup: ## Setup environment. Copy the data to GCS and create some BQ.
	@gsutil -m cp -R datasets/. gs://${PIPELINE_BUCKET}/data/ && \
	bq mk --dataset ${PIPELINE_PROJECT_ID}:${PIPELINE_DATASET}

run-simple-batch: ## Run simple batch pipeline.
	@pdm run python -m src.pipelines.simple_batch_example.pipeline \
		--input gs://${PIPELINE_BUCKET}/data/users.csv \
		--output ${PIPELINE_PROJECT_ID}:${PIPELINE_DATASET}.users \
		--temp_location gs://${PIPELINE_BUCKET}/temp/ \
		--staging_location gs://${PIPELINE_BUCKET}/stage/ \
		--project ${PIPELINE_PROJECT_ID} \
		--region ${PIPELINE_REGION} \
		--runner ${BEAM_RUNNER}

run-multi-pardo: ## Run multiple batch pipeline.
	@pdm run python -m src.pipelines.multiple_output_pardo.pipeline \
		--output gs://${PIPELINE_BUCKET}/output/multiple_output_pardo/result \
		--temp_location gs://${PIPELINE_BUCKET}/temp/ \
		--staging_location gs://${PIPELINE_BUCKET}/stage/ \
		--project ${PIPELINE_PROJECT_ID} \
		--region ${PIPELINE_REGION} \
		--runner ${BEAM_RUNNER}

run-wordcount: ## Run word count pipeline.
	@pdm run python -m src.pipelines.wordcount.pipeline \
		--output gs://${PIPELINE_BUCKET}/output/wordcount/result \
		--temp_location gs://${PIPELINE_BUCKET}/temp/ \
		--staging_location gs://${PIPELINE_BUCKET}/stage/ \
		--project ${PIPELINE_PROJECT_ID} \
		--region ${PIPELINE_REGION} \
		--runner ${BEAM_RUNNER}

run-window: ## Run simple window pipeline.
	@pdm run python -m src.pipelines.window_example.pipeline \
		--input_topic
		--output_table ${PIPELINE_PROJECT_ID}:${PIPELINE_DATASET}.window_result \
		--temp_location gs://${PIPELINE_BUCKET}/temp/ \
		--staging_location gs://${PIPELINE_BUCKET}/stage/ \
		--project ${PIPELINE_PROJECT_ID} \
		--region ${PIPELINE_REGION} \
		--runner ${BEAM_RUNNER}

run-side-input: ## Run side input example pipeline.
	@pdm run python -m src.pipelines.side_input_example.pipeline \
		--output gs://${PIPELINE_BUCKET}/output/side_input_example/result \
		--num_groups 5 \
		--temp_location gs://${PIPELINE_BUCKET}/temp/ \
		--staging_location gs://${PIPELINE_BUCKET}/stage/ \
		--project ${PIPELINE_PROJECT_ID} \
		--region ${PIPELINE_REGION} \
		--runner ${BEAM_RUNNER}

run-pubsub: ## Run pubsub example pipeline.
	@pdm run python -m src.pipelines.pubsub_example.pipeline \
		--output_topic xxxx \
		--temp_location gs://${PIPELINE_BUCKET}/temp/ \
		--staging_location gs://${PIPELINE_BUCKET}/stage/ \
		--project ${PIPELINE_PROJECT_ID} \
		--region ${PIPELINE_REGION} \
		--runner ${BEAM_RUNNER}

run-dataframe-taxi: ## Run taxi example pipeline using dataframe.
	@pdm run python -m src.pipelines.dataframe_taxi_example.pipeline \
		--output gs://${PIPELINE_BUCKET}/output/dataframe_taxi_example/result \
		--temp_location gs://${PIPELINE_BUCKET}/temp/ \
		--staging_location gs://${PIPELINE_BUCKET}/stage/ \
		--project ${PIPELINE_PROJECT_ID} \
		--region ${PIPELINE_REGION} \
		--runner ${BEAM_RUNNER}

run-dataframe-flight: ## Run taxi example pipeline using dataframe.
	@pdm run python -m src.pipelines.dataframe_flight_example.pipeline \
		--start_date 2012-12-24 \
		--end_date 2012-12-25 \
		--output gs://${PIPELINE_BUCKET}/output/dataframe_flight_example/result \
		--temp_location gs://${PIPELINE_BUCKET}/temp/ \
		--staging_location gs://${PIPELINE_BUCKET}/stage/ \
		--project ${PIPELINE_PROJECT_ID} \
		--region ${PIPELINE_REGION} \
		--runner ${BEAM_RUNNER}
