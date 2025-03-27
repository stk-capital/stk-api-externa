from pydantic import BaseModel
def custom_encoder(obj):
    """
    Recursively encodes Pydantic models into dictionaries,
    keeping datetime objects as datetime instances.
    """
    if isinstance(obj, BaseModel):
        obj_dict = obj.model_dump(mode='python', exclude_none=True, by_alias=True)
        return {k: custom_encoder(v) for k, v in obj_dict.items()}
    elif isinstance(obj, dict):
        return {k: custom_encoder(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple, set)):
        return [custom_encoder(v) for v in obj]
    else:
        return obj