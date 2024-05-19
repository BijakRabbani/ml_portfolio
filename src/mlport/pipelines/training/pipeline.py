from kedro.pipeline import Pipeline, node
from functools import partial, update_wrapper
import yaml
from .nodes import (build_train_data, train)


def create_pipeline(model_name, **kwargs):
    with open('conf/base/catalog.yml') as stream:
        data_info = yaml.safe_load(stream)
    pipeline = []
    model_info = data_info[model_name]
        
    # Extract input data
    input_data = model_info['function']['input']
    target_data = model_info['function']['target']
    input_node = input_data + [target_data]
    
    # Extract dataset config and apply to build_train_data
    config = model_info['function']
    build_train_data_partial = partial(build_train_data, config=config)
    update_wrapper(build_train_data_partial, build_train_data)

    # Build and insert node
    node_function = node(build_train_data_partial,
                        input_node,
                        'train_dataset',
                        name='train_dataset',)
    pipeline.append(node_function)
    node_train = node(train,
                ['train_dataset'],
                model_name,
                name=f'train_{model_name}',)
    pipeline.append(node_train)
        
    return Pipeline(pipeline)