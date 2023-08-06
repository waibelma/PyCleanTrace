Python Code to Clean Academic TRACE data following the procedure outlined in [Dick-Nielsen & Poulsen (2019)](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3456082)
-------------
**PyCleanTrace** is an integrated Python code that facilitates the i) reading of the raw data from the text files, ii) the concatenation of the annual TRACE datasets, iii) the implementation of the cleaning and correction steps as in Dick-Nielsen & Poulsen (2019), and iv) The generation of relevant  microstructure variables and the implementation of additional cleaning steps standard in the literature (compare [Bessembinder (2019)](https://onlinelibrary.wiley.com/doi/abs/10.1111/jofi.12694)).

The original cleaning code by Dick-Nielsen & Poulsen (2019) is written in SAS. **Clean_Academic_TRACE** is meant to provide an open-source alternative to facilitate the data management when working with Academic TRACE in Python.

Note: This version is fully functional and tested on OS systems. In a next step Windows comatibility will be assured and further tests will be implemented. A parallelisation step is already integrated but not yet applied. This version is preliminary.  Any feedback is highly welcome.

If you use the code in PyCleanTrace for a publication or project, please cite PyCleanTrace as follows:

```
@software{PyCleanTrace,
  author = {Martin Waibel},
  title = {Cleaning Academic Trace in Python: PyCleanTrace},
  url = {https://martinwaibel.com/project/example/trace_cleaning_python/},
  version = {0.0.1},
  date = {2023-08-06},
}
```
-------------

Instructions for  running **Clean_Academic_TRACE**:

1) Install git on your computer. [git download instructions](https://www.atlassian.com/git/tutorials/install-git).

2) Clone the repository to a local folder of your choice by using [this](https://github.com/waibelma/Clean-Academic-TRACE-data.git) link.

3) Before placing the raw data: Navigate in the terminal into the **src** folder and initiate the data generation process via **sudo python build_TRACE.py**

	3.1) Upon the first run, the relevant subfolder directories will be created and the code will stop with the instruction to place the raw data (TRACE and MERGENT) into the respective  **original_data** folder

4) Place the raw TRACE data (in an unzipped folder on the annual level) in the folder  src/original_data/academic_TRACE/TRACE_raw

   	4.1) Note: zip system data need to be removed as this will otherwise interrupt the code

6) Place the Mergent FISD data in the folder **src/original_data/Mergent_FISD**. There has to be one dataset for bond issue information (named issue_data.pkl) and one dataset for the rating information (named ratings.pkl). For the structure and variable names of the dataset please refer to the sample datasets **illustration_issue_data.csv** and **illustration_ratings.csv**. These datasets can be found in the folder **src/original_data/Mergent_FISD/sample_data**

7) Adjust the start and end year of the final sample in the dictionary in **build_TRACE.py** according to your preference. The start year is determined by the year of the first TRACE folder and the end year by the year of the last folder, respectively (do not place years in the original data folder that are not used)

8) Re-run  **sudo python build_TRACE.py** in the **src** folder. The process will start automatically and read, concatenate, and clean the data. In a last step relevant microstructure variables will be generated. 

   7.1) The code loops through the available years backwards in time (starting with the last year) and reads the raw data, performs the cleaning steps and concatenates the yearly datasets to one final cleaned and concatenated TRACE dataset.


Description of subfolders
-------------

1) At the source, the project contains a **src** folder and a **bld** folder. The **src** folder contains all scripts as well as the raw input data. The **bld** folder contains all processed (intermediate and final) data

2)  **build_TRACE.py**: This script wraps all subscripts and dictates the order in which processes are executed. The script contains a dictionary with the relevant specifications, such as time span, selected variables, etc.

2)  **general_functions.py**: This script specifies all functions that automatically set up the relevant folders and directories where the processed datasets are stored. It also checks whether the required data already exists and only runs the code if the final data is not yet there

3)   **read_TRACE.py**: This script defines all functions necessary to read in the raw data files from the respective subfolders. It further specifies the concatenation of the daily txt. files to a yearly dataset that is saved in an intermediate step

4)  **clean_TRACE.py**: This script specifies all cleaning steps. It includes general cleaning steps that handle the conversion of the raw data types and specific cleaning steps that follow what is common in the literature (compare Bessembinder et al. (2018)).

5)  **read_bond_background_TRACE.py**: This script reads out the additional bond background information that ships in with TRACE

5)  **concatenate_merge_TRACE_MERGENT.py**: This script manages the concatenation of the yearly raw data and merges relevant bond characteristics from MERGENT. It also handles bond inclusion/exclusion based on bond characteristics

5)  **prepare_variables.py**: This script outlines the construction of relevant microstructure variables (such as the USD trading volume)








