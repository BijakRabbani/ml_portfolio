from kedro.pipeline import Pipeline, node
import yaml
from . import nodes
from functools import partial, update_wrapper


def create_pipeline(field_list=None, **kwargs):
    with open('conf/base/feature_catalog.yaml') as stream:
        data_info = yaml.safe_load(stream)
    
    pipeline = []
    for feature in data_info:
        
        # Skip data
        if field_list is not None:
            if feature not in field_list:
                continue
        
        # Extract function name and args, build partial function
        function_name = data_info[feature]['function']['name']
        input_data = data_info[feature]['function']['input']
        if 'args' in data_info[feature]['function'].keys():
            args = data_info[feature]['function']['args']
        else:
            args = {}
        function = getattr(nodes, function_name)
        partial_func = partial(function, **args)
        update_wrapper(partial_func, function)
    
        # Build and insert node
        node_function = node(
            partial_func,
            input_data,
            feature,
            name=feature,
            )
        pipeline.append(node_function)
        
    return Pipeline(pipeline)