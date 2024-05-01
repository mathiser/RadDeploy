# RADDEPLOY HAS MOVED TO [GITLAB](https://gitlab.com/aarhus-radonc-ai/repos/raddeploy)


# Note !!
This project has previously been developed under the name "RadDeploy", but to avoid confusion with other projects it has been renamed to RadDeploy. As the name "RadDeploy" has been used many, places thorough refactoring of the naming is in the pipeline.

# RadDeploy
RadDeploy is an extendable micro-service-oriented framework to executed containerized workflows on DICOM files.
Though RadDeploy can be extensively configured, it is designed to provide basic functionality with as little user configuration
as possible - even though this sometimes may come at the price of reduced performance.

## Intended Use
RadDeploy is provided as is and without any responsibility or warrenty by the authors. The author does not considered it a medical device under EU's Medical Device Regulation (MDR),
as it only facilitate recieving of, execution of docker containers on and finally sending of DICOM files. RadDeploy in it self does not manipulate any of the received data. 
Operations on data are limited to storing, lossless compression, mounting to containers, receiving and sending of dicom files. 

If used in a clinical/research setting, beaware that the docker containers should comply to MDR or any local legislation regarding medical devices.

## Get started
### Dependencies
RadDeploy should be able run in any environment with [Docker](https://www.docker.com/get-started/) installed. If the executed containers require GPU-support, [NVIDIA container toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html)
must be installed with appropriate dependencies and a compatible GPU installed.

### Setting up RadDeploy
One you have dependencies set up, you can use the prebuild docker images and be up and running in a minute with the commands below:
```
mkdir RadDeploy
cd RadDeploy
wget https://raw.githubusercontent.com/mathiser/RadDeploy/main/docker-compose.yaml.example
wget https://raw.githubusercontent.com/mathiser/RadDeploy/main/.env.example
mv docker-compose.yaml.example docker-compose.yaml
mv .env.example .env
docker compose up -d
```

If the services start successfully, you can inspect the logs of all services using `docker compose logs -f` and logs of a specific service with `docker compose logs -f {service name}`. 
Now you should have a DICOM receiver running by default on `localhost:10000` with the ae title: `RadDeploy`.

### Updating RadDeploy
If you want to update RadDeploy to a more recent version, go ahead an run:
```
docker compose pull
docker compose up -d
```

### Docker compose mounts
The `fingerprinter`-service should have a folder container flow-definitions (see next section) mounted. By defaults this this mount is: `./mounts/flows:/opt/DicomFlow/flows`
The `file_storage`-service may have static tars mounted to `/opt/DicomFlow/static`.

In all services, logs are put in `/opt/DicomFlow/logs`, and can be mounted to a permanent dir if needed.

If you want to spin up a grafana dashboard, you need to have flow_tracker running, and 
to give grafana access to it's database, which by default is `./mounts/flow_tracker:/opt/DicomFlow/database`


### Defining a Flow
In RadDeploy, a "Flow" refers to the full of path of a dicom input through model execution an dicom sending to a different endpoint. Flows are defined in yaml files (one per flow) and mounted to `fingerprinter` in `/opt/DicomFlow/flows`.

Below is an example of the simplest possible flow. It triggers if any received dicom file is a CT. The resulting folder is not sent anywhere.
```
triggers:
  - Modality: CT
models:
  - docker_kwargs:
      image: hello-world
destinations: []
```
#### triggers
`triggers` specify which dicom tags will initiate the `models`. Matching to dicom tags is done in two "dimensions":
- Each trigger (dict in the `triggers` list) must be matched **somewhere in the received dicom files**
- Regex patterns (the values for each dicom tag) must **all** match with in the trigger. "~" is used as exclusion pattern.
If found, the flow wil never be triggered.

An example of a more complex triggers is given:
```
triggers:
  - Modality: ["CT"]                                   # Matches if CT is contained in "Modality"
    SeriesDescription: ["Head|Neck|Brain", "Contrast"] # (Head or Neck or Brain) AND (Contrast) must be present in
                                                       # all CTs
    StudyDescription: ["(?i)RT", "~FLAIR"]             # Study description where (CT) and (Head or Neck or Brain) must
                                                       # include "RT" (insensitive to case), but never "FLAIR"
  - SOPClassUID: ["1.2.840.10008.5.1.4.1.1.4"]         # SOPClassUID for MRI
    StudyDescription: ["AutoSegProtocol"]              # Must include "AutoSegProtocol"
```

Regex can be tricky, so go ahead and try out your patterns in a [regex tester](https://regex101.com/)


#### models
In `models` the actual docker containers are defined. `models` implements a DAG workflow, where two keywords are reserved:
"src" denotes the received file from the storescp, and "dst" denotes everything that should be sent on to the defined
destinations. Where "src" may be used multiple time, "dst" can only be used once. Below is a more comprehensive definition of `models`, 
where five containers (folds of a segmentation model, for instance) are run in parallel across the available consumers, and
finally merged in a container, which takes the output of the five containers as input. Furthermore, `timeout`, `pull_before_exec` 
and `static_mounts`. Timeout is the allow runtime of a container. If `pull_before_exec` is set, the image is tried pulled from
docker hub before each execution. `static_mounts` is a dictionary of files provided by `static_storage`-service, and can be used
to put sensitive files in containers, which are not build into the container image.

```
models:
  - docker_kwargs: # All variables in `docker_kwargs` are actually configured through the containers.run interface of 
                   # docker-py (https://docker-py.readthedocs.io/en/stable/containers.html), and thus almost any value
                   # can be used (some are not allowed per design)      
      image: busybox
      command: sh -c 'touch /output/SEG_0'
    input_mounts:
      src: /input
    output_mounts:
      seg0: /output

  - docker_kwargs:
      image: busybox
      command: sh -c 'touch /output/SEG_1'
    input_mounts:
      src: /input
    output_mounts:
      seg1: /output

  - docker_kwargs:
      image: busybox
      command: sh -c 'touch /output/SEG_2'
    input_mounts:
      src: /input
    output_mounts:
      seg2: /output

  - docker_kwargs:
      image: busybox
      command: sh -c 'touch /output/SEG_3'
    input_mounts:
      src: /input
    output_mounts:
      seg3: /output

  - docker_kwargs:
      image: busybox
      command: sh -c 'touch /output/SEG_4'
    input_mounts:
      src: /input
    output_mounts:
      seg4: /output

  - docker_kwargs:
      image: busybox
      command: sh -c 'cp /input*/* /output/; ls -la /output; sleep 10'
    input_mounts:
      seg0: /input0
      seg1: /input1
      seg2: /input2
      seg3: /input3
      seg4: /input4
    output_mounts:
      dst: /output
    pull_before_exec: True  # Try to pull "busybox" from hub.docker.com before every execution. 
    timeout: 1800  # default timeout is 1800 seconds. If the container is running for longer, it will be terminated forcefully. This is to avoid hanging container jobs, which obstruct the queue.
    static_mounts: 
      - "AwesomeModel1:/model1" # Will make AwesomeModel1 from static_storage available in container's /model
      - "AwesomeModel2:/model2" # giving the container access to static model. 
                               # Note "AwesomeModel2" must be mounted to the static_storage service to
                               # "/opt/DicomFlow/static" as tar: "/opt/DicomFlow/static/AwesomeModel2.tar"
                               # Note that you cannot specify "ro" or "rw" - only one colon should be in the string
                               # splitting src_filename:dst_in_container

```
#### destinations
`destinations` contains a list of dicom nodes to send all files from the last container's `output_dir` to. Below is an example:
```
destinations:
  - host: localhost
    port: 10001
    ae_title: STORESCP
  - host: storage.some-system.org
    port: 104
    ae_title: FANCYSTORE
```
#### Other variables
Furhtermore the following variables can be set in the flow definition

##### name
`name` of the Flow. Defaults to "" (empty string)

##### version
`version` of the Flow. Defaults to "" (empty string)

##### priority
`priority` can be set to an integer between 0 and 5, where 5 is highest priority. 0 is default. When consumers are receiving a new flow to be run, the available flow with highest priority with run. 
Note, that priority makes flows jump the queue, but NOT pause running flows.

##### extra
`extra` is a dictionary that will follow the flow all the way. It can be used for custom services etc..

##### return_to_sender_on_port
`return_to_sender_on_port` is a list of integer ports. Can be used to directly send back to the scu (sender) of the flow on a specific port. 

### Full Flow example
```
name: "FancyFlow"
version: "1.02
priority: 4
return_to_sender_on_ports:
  - 104
triggers:
  - Modality: ["CT"] 
    SeriesDescription: ["Head|Neck|Brain"] 
  - SOPClassUID: ["1.2.840.10008.5.1.4.1.1.4"] 
    StudyDescription: ["AutoSegProtocol"]
models:
  - docker_kwargs:
      image: busybox
      command: sh -c 'echo $CONTAINER_ORDER; cp -r /input /output/'
      environment:
        CONTAINER_ORDER: 0
    gpu: False
    input_mounts:
      src: /input
    output_mounts:
      first: /output
    
  - docker_kwargs:
      image: busybox
      command: sh -c 'echo $CONTAINER_ORDER; cp -r /input /output/'
      environment:
        CONTAINER_ORDER: 1
    gpu: True
    input_mounts:
      first: /input
    output_mounts:
      dst: /output
    pull_before_exec: True
    timeout: 1800
    static_mounts: "AwesomeModel1:/model"
destinations:
  - host: localhost
    port: 10001
    ae_title: STORESCP
  - host: storage.some-system.org
    port: 104
    ae_title: FANCYSTORE
extra:
  INTERESTING_PARAMETER: FooBar
  USED_FOR_CUSTOM_SERVICE: Niiice
```

## Configuration of services
All services in RadDeploy are extensively configurable, but do only change something if you know what you do. 

If you would like to change a configuration, you have two options. 
- Mount a .yaml file with the desired variables into the containers directory `/opt/DicomFlow/conf.d`.
- Set an environment variable in the docker compose file.

Configs are (over)loaded in the order:
- Alphabetically from yaml files in `/opt/DicomFlow/conf.d` (Defaults are in `00_default_config.yaml`)
- Environment variables (order of occurrence).

Configuration variable names should be kept in capital letters to be able to easily identify them.

### Shared configurations
RabbitMQ must be configured on all services using the following variables:
```
# RabbitMQ
RABBIT_HOSTNAME: "localhost"
RABBIT_PORT: 5672
```

Log level can be set with:
```
# Logging
LOG_LEVEL: 20
```

### Service specific configurations
In principle all variables in `/opt/DicomFlow/conf.d/00_default_config.yaml`, but probably should not.
Below is a list of variables which should be adapted to the specific environment

#### STORESCP
##### IP/Hostname and port of SCP 
The DICOM receiver can be adapted with the following variables. Defaults given here.
```
# SCP
AE_TITLE: "DicomFlow"
AE_HOSTNAME: "localhost"
AE_PORT: 10000
PYNETDICOM_LOG_LEVEL: 20
```
##### TAR_SUBDIR
By default, the receiver writes dicom files into a flat directory,
which is mounted into the containers input mountpoint. 
This is set with the default: `TAR_SUBDIR: []`, which will result in the following container input:
- /input/
  - file1.dcm
  - file2.dcm
  - file3.dcm
  - file4.dcm
  - file5.dcm
  - ...

When models are operating on multi-modal input data, it may be desirable to separate files into a directory
already when they are received. To do this, `TAR_SUBDIR` can contain a list of dicom tags, to be used as sub-folders.
As an example:
```
TAR_SUBDIR: 
  - SOPClassUID
  - SeriesInstanceUID
```
Which will results in something like:
- /input/
  - 1.2.840.10008.5.1.4.1.1.2
    - 1.1.1.1.1
      - CT1.dcm
      - CT2.dcm
      - ...
  - 1.2.840.10008.5.1.4.1.1.4
    - 1.1.1.1.1
      - MR_T1.dcm
      - ...
    - 2.2.2.2.2
      - MR1_T2.dcm

#### Consumer
`CPUS` is a count of CPU-threads RadDeploy. Should be an integer

`GPUS` is a list of physical GPUs that are at disposal for flow execution. 
It is the GPU-index which can be found in `nvtop` or `nvidia-smi`

For instance:
```
GPUS: 
  - 0
```
It can also be provided as a space seperated string in an environment variable like `GPUS=0 2 5`. 
Defaults to an empty list.

## Dashboard
If you want to set up the grafana dashboard, make sure that the grafana service has access to `flow_tracker`'s
`/opt/DicomFlow/database/database.sqlite` with the following two mounts:
- **flow_tracker**: `./mounts/flow_tracker:/opt/DicomFlow/database:rw`
- **dashboard**: `./mounts/flow_tracker:/var/lib/grafana/database:ro`

You can then go to http://localhost:3000.
When you setup a new datasource, use sqlite and set the path to the database to: Path `/var/lib/grafana/database/database.sqlite`

You can use the SQL query: `SELECT * FROM rows` and format ts, Dispatched, Finished and Sent as time objects. 

### Status
- 0: Pending Flow (waiting to be executed)
- 1: Flow is being scheduled
- 2: Flow is running
- 3: Flow finished (but not yet sent to destinations)
- 4: Sending output files
- 5: Sent output files
- 400: Failed

In the table settings, these status codes can be mapped to human readable strings.

# Firewall
If you are using a firewall, you should make sure that the SCP and SCU ports are open.
By default they are 10000 and 10005, respectively
