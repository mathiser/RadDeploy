from RadDeployLib.data_structures.flow import Destination


def generate_destination_from_event(event):
    return Destination(host=event.assoc.requestor.address,
                       port=event.assoc.requestor.port,
                       ae_title=event.assoc.requestor._ae_title)
