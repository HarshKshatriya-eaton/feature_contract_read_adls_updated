ileads_lead_generation
==============================

lead generation module

Project Organization
------------
    ├── README.md          <- The top-level README for developers using this project.
    │
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │    
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                        generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    │    
    ├── <repo_name>
    │   ├── __init__.py    <- Makes src a Python module
    │   └── steps          <- Steps to be executed in the pipeline
    │   |  └── ingest
    │   |  └── transform
    │   |  └── train
    │   |  └── inference
    │   |
    │   └── utils         <- utility functions specific to <algorithm>
    │
    ├── tests
    │
    ├── README.md
    │    
    ├── updateversion.py
    │
    ├── version.py
    │
    └── .gitignore
    │                       
    │   
    │
    ├──  docker-compose.yaml    <- yaml file defining service, networks, volumes for a containerized workspace
    │
    ├── .devcontainer.json  <- Describes how VS Code should start the container and what to do after it connects.
    │
    └── .env                <- File containing key value pairs of all the environment variables required by your application.


--------

<p><small>Project based on the <a target="_blank" href="https://github.com/etn-electrical/bas-data-science-quickstart-repo.git">BAS Data Science template</a>.</small></p>