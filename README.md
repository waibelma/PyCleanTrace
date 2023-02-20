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


   6.1) The code loops through the available years backwards in time (starting with the largest year) and reads the raw data, performs the cleaning steps
        and concatenates the yearly datasets to one final cleaned and concatenated TRACE dataset.


Description
-------------
Below we list a description of the toolbox structure as well as a short description of the file content.








