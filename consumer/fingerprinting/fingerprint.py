from database.models import Fingerprint
from dicom_networking.scp import Assoc

def fast_fingerprint(fp: Fingerprint, assoc: Assoc):
    """
    Fast fingerprint starts off by checking if all SopClassUIDs are present in assoc.series_instances.
    If this is not the case, there is no way a fingerprint can match
    """
    sop_class_uid_exacts = set([trigger.sop_class_uid_exact for trigger in list(fp.triggers)])
    available_sop_class_uids = set([series_instance.sop_class_uid for _, series_instance in assoc.series_instances.items()])

    return sop_class_uid_exacts.issubset(available_sop_class_uids)

def slow_fingerprint(fp: Fingerprint, assoc: Assoc):
    """
    This can run on it self, but is preferably run after fast_fingerprint.
    Performs full fingerprint
    :param fp:
    :param assoc:
    :return list of SeriesInstances if fp matches all. Else return None
    """
    matching_series_instances = []
    triggers = [trigger for trigger in list(fp.triggers)]
    series_instances = [series_instance for _, series_instance in assoc.series_instances.items()]
    for trigger in triggers:
        for series_instance in series_instances:
            # Check all "in-patterns"
            if (trigger.sop_class_uid_exact is None or trigger.sop_class_uid_exact == series_instance.sop_class_uid) and \
               (trigger.series_description_pattern is None or trigger.series_description_pattern in series_instance.series_instance_uid) and \
               (trigger.study_description_pattern is None or trigger.study_description_pattern in series_instance.study_description):
                # Check all "out-patterns"
                if trigger.exclude_pattern is None or \
                    (series_instance.sop_class_uid is None or trigger.exclude_pattern not in series_instance.sop_class_uid) and \
                    (series_instance.series_instance_uid is None or trigger.exclude_pattern not in series_instance.series_instance_uid) and \
                    (series_instance.study_description is None or trigger.exclude_pattern not in series_instance.study_description):
                    matching_series_instances.append(series_instance)
                    break
        else:
            return None

    return matching_series_instances