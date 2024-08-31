from code.core.pipeline.my_pipeline import Pipeline

if __name__ == "__main__":

    pipeline = Pipeline()
    pipeline.initialiseSpark("local")
    pipeline.runPipeline()

