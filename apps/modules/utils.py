import yaml


def get_fields_from_sigma(sigma_text: str) -> set[str]:
    try:
        sigma_dict = yaml.safe_load(sigma_text)
    except yaml.YAMLError:
        return set()
    if not isinstance(sigma_dict, dict):
        return set()
    detection = sigma_dict.get("detection")
    if not isinstance(detection, dict):
        return set()

    fields = set()
    for _, statements in detection.items():
        if not isinstance(statements, list):
            statements = [statements]
        for statement in statements:
            if not isinstance(statement, dict):
                continue
            for field in statement.keys():
                field = field.split("|")[0]
                fields.add(field)
    return fields
