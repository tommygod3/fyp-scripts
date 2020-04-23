# fyp-scripts
Collection of processing scripts used in my final year project

## Instructions to run
The environment is specifically configured to run on [JASMIN](http://jasmin.ac.uk/). It uses things specific to JASMIN such as the [CEDA Archive](https://www.ceda.ac.uk/services/ceda-archive/) and [LOTUS](http://www.jasmin.ac.uk/services/lotus/). Therefore, These scripts can only be run in their current form on JASMIN.  

To run this on JASMIN you will need write access to an Elasticsearch cluster and a JASMIN account with read access on the [CEDA Archive](https://accounts.jasmin.ac.uk/).  

You will also need to change the `elasticsearch_url`, `sen2cor_path` and `model_weights` settings in the [`environment.json`](./environment.json) as described in the [dependencies](##Dependencies) section.  

## Dependencies
All Python package dependencies are listed in the environment.yml.  

An [Elasticsearch](https://www.elastic.co/) cluster is required to write data outputs to. The url to a cluster you have write access to is required in the [`environment.json`](./environment.json).  

In terms of external dependencies, a path to a [Sen2Cor](http://step.esa.int/main/third-party-plugins-2/sen2cor/sen2cor_v2-8/) installation is required in the [`environment.json`](./environment.json).  

As well as this, the [`bigearthnet-models-tf`](https://gitlab.tu-berlin.de/rsim/bigearthnet-models-tf/tree/cec5ae5eb0e55a0d15f24abd426b3158e6e8e130) repository should be downloaded and installed using `pip` locally.  

In addition, the [`model_weights`](bigearth.net/static/pretrained-models/BigEarthNet-19_labels/ResNet152.zip) also need to be downloaded and the path set in the [`environment.json`](./environment.json).  
