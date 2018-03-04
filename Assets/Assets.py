import json
import querybody as queryBody


def get_tags(classifier, classifier_id):
    return json.dumps(queryBody.query_get_tags(classifier, classifier_id))


def get_timeseries_tags():
    return json.dumps(queryBody.query_get_timeseries_tags())


def put_tags(classifier, classifier_id, new_value):
    return str(queryBody.query_put_tags(classifier, classifier_id, new_value))


def delete_tags(classifier, classifier_id, old_value):
    return str(queryBody.query_delete_tags(classifier, classifier_id, old_value))
