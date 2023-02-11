Python Code to Clean Academic TRACE data following the procedure outlined in [Dick-Nielsen & Poulsen (2019)](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3456082)
-------------
**Clean_Academic_TRACE** is an integrated Python code that facilitates the i) reading of the raw data from the text files, ii) the concatenation of the individual TRACE datasets, iii) the implementation of the cleaning and correction steps as on Dick-Nielsen & Poulsen (2019), and iv) The generation of relevant variables and the implementation of additional cleaning steps. 

The original cleaning code by Dick-Nielsen & Poulsen (2019) is written in SAS. **Clean_Academic_TRACE** is meant to provide an open-source alternative to facilitate the data management when working with Academic TRACE in Python.

-------------

Instructions for  running **Clean_Academic_TRACE**:

1) Install git on your computer. [git download instructions](https://www.atlassian.com/git/tutorials/install-git)

2) Clone the repository to a local folder of your choice by using [this](https://github.com/waibelma/Clean-Academic-TRACE-data.git) link

3) Place the raw TRACE data (in annual -zip folders) in the folder  src/original_data/academic_TRACE/TRACE_raw

4) Place the Mergent FISD data in the folder src/original_data/Mergent_FISD. There has to be one dataset for bond issue information and one dataset for the rating information. For the structure of the dataset please refer to the sample datasets **issue_data_sample.pkl** and **ratings_sample.pkl**

5) Change the **project_path** in the file **build_TRACE.py** into your corresponding path

6) Navigate in the terminal into the **src** folder and initiate the data generation process via **python build_TRACE.py**
   6.1) 


Description
-------------
Below we list a description of the toolbox structure as well as a short description of the file content.

**toolbox_specs.py: Specification of the final feature dataset**
description: Defines the total time range of the feature data and initiates the construction of the feature dataset

**features (monthly): Folder containing the scripts for the monthly version of  feature dataset construction**

**features (daily): Folder containing the scripts for the daily version of  feature dataset construction (note: this version is currently under construction and depends on DASK)**

**run_mnthly: Script wrapping the necessary steps to construct the feature data**
description: Invokes the data retrieval step, and constructs the features listed in Gu et al. (2020)

**util.py: Select the features that are to be constructed and performs the variable management to maintain only relevant variables**
description: Manage which features should be constructed

**retrieve_data: Folder containing the scripts to download and process Compustat and CRSP data**

**retrieve_comp: Specify the data retrieval steps for the Compustat data**
description: Download the Compustat data (annual \& quarterly) from WRDS and save the raw data

**retrieve_crsp: Specify the data retrieval steps for the CRSP data**
description: Download the CRSP data (daily \& monthly) from WRDS and save the raw data

**retrieve_macro: Specify the data retrieval steps for the macro data**
description: Download the macro data from publicly available sources

**retrieve_data_all: Wrapper function to manage the retrieval steps using SQL code**
description: Specify the SQL code necessary to download the raw data from WRDS

**general: Folder containing general functions to set up the toolbox infrastructure**
description: Establish the connection between the local machine and the WRDS database as well as saving, loading and merging functions

**connection: Create the connection to the WRDS database using the user credentials**
description: Set up the pgpass file and read in the user credentials from the .env file.

**general_functions.py: Define the loading and saving functions as well as the merging of Compustat and CRSP datasets**

**build_features: Folder defining the individual features and managing the dataset creation steps**
description: Define all 94 features in line with Gu et al. (2020) and Green et al. (2017)

**define_features.py: Define the individual features**
description: Create the 94 features used in  Gu et al. (2020) and define the individual helper functions used to construct the features

**merge_data_build_features: Execute the individual steps to merge the dataset and create the features**

**clean_data_features: Perform cleaning steps for individual variables**




Citation
-------------
In case you are using **ML_toolbox** for your own research please don't forget to cite it with 

 ```
@Unpublished{ML_toolbox2022,
 Title  = {ML_toolbox - A Machine Learning Toolbox in Asset Pricing},
 Author = {d’Avernas, Adrien,  Sichert, Tobias, Waibel Martin, and Chunjie Wang},
 Year   = {2022},
 Url    = {https://github.com/AAs-sudo/asset_pricing},
}
 ```
