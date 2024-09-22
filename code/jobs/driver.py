from core.pipeline.my_pipeline import Pipeline

if __name__ == "__main__":

    pipeline = Pipeline()
    pipeline.initialise_spark("local")
    pipeline.run_pipeline()

