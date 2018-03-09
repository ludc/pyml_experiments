from Experiment import Experiment

def test_can_build_Experiment_without_writer():
    # don't pass any writer. The goal is to ensure that it doesn't throw
    Experiment(arguments= {"foo": "bar"})

