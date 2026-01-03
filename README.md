# Project Setup Instructions

## 1. Environment Setup
It is recommended to use a virtual environment to keep dependencies isolated.
It is necesarry to use Python 3.8, 3.9 or 3.10.
Also, hadoop needs to be installed in the root folder of the project, next to a spark_ivy_cache folder.

### Create and Activate Virtual Environment
Open your terminal in the project root and run:

```bash
# Create the virtual environment
python -m venv .venv

# Activate the virtual environment
# Windows (Command Prompt):
.venv\Scripts\activate.bat
# Windows (PowerShell):
.venv\Scripts\Activate.ps1
# Linux/Mac:
source .venv/bin/activate

#Running the reader and writer scripts from src/workload:
python src/workload/reader.py --city Paris --id 26
The arguments can be ommited for the city, in which case all the values in the table will be printed.
Also, it is not necesarry for all the arguments to be used.

python src/workload/writer.py --city Paris --id 26 --value 450.5
