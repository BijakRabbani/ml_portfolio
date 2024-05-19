"""Project pipelines."""
from __future__ import annotations
from typing import Dict

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline
from mlport.pipelines import raw, feature, training


def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    # pipelines = find_pipelines()
    pipelines = {}
    pipelines['raw'] = raw.create_pipeline()
    pipelines['feature'] = feature.create_pipeline()
    pipelines['training'] = training.create_pipeline()
    pipelines["__default__"] = sum(pipelines.values())
    return pipelines


def load_pipeline(name, **kwargs) -> Dict[str, Pipeline]:
    """
    Load pipeline
    """
    match name:
        case 'raw':
            return raw.create_pipeline(**kwargs)
        case 'feature':
            return feature.create_pipeline(**kwargs)
        case 'training':
            return training.create_pipeline(**kwargs)
